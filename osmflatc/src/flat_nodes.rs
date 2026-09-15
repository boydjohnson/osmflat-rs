//! Flat mmap'd node-location store ("flat nodes"), indexed by OSM node id.
//!
//! An alternative to the `NodeIdToLonLat` RocksDB column family for
//! planet-scale ingests: one 8-byte slot per node id in a sparse file, so a
//! lookup is a single O(1) memory read instead of an LSM point get. Planet
//! node ids are ~90% dense, which makes the file barely larger than the data;
//! for small regional extracts (whose ids are scattered across the whole
//! planet id space) it touches far more pages than RocksDB would write, so it
//! is opt-in via `--flat-nodes`.
//!
//! Holes in the sparse file read as zero. A slot value of zero must therefore
//! mean "no such node", so coordinates are stored XOR `i32::MIN`: the zero
//! slot then decodes to `(i32::MIN, i32::MIN)`, which no scaled coordinate can
//! produce (the scaled lon/lat range fits inside `i32` with room to spare),
//! and a real node at exactly (0, 0) round-trips unambiguously.

use memmap2::MmapMut;
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const SLOT_BYTES: u64 = 8;

/// Initial logical file length (sparse, so this costs no disk). Doubles on
/// demand; a planet ingest (max id ~2^34) performs ~10 remaps total.
const INITIAL_LEN: u64 = SLOT_BYTES * (1 << 24);

/// Ids above this are refused: the logical file length is `id * 8`, and a
/// wildly out-of-range id (corrupt input) would otherwise create an absurd
/// mapping. 2^40 leaves ~60x headroom over 2026 planet ids at an 8 TiB
/// logical (still sparse) ceiling.
const MAX_NODE_ID: i64 = 1 << 40;

fn encode(lon: i32, lat: i32) -> u64 {
    ((((lon ^ i32::MIN) as u32) as u64) << 32) | (((lat ^ i32::MIN) as u32) as u64)
}

fn decode(v: u64) -> Option<(i32, i32)> {
    if v == 0 {
        return None;
    }
    let lon = ((v >> 32) as u32 as i32) ^ i32::MIN;
    let lat = (v as u32 as i32) ^ i32::MIN;
    Some((lon, lat))
}

pub struct FlatNodes {
    file: File,
    map: RwLock<MmapMut>,
}

impl FlatNodes {
    /// Create the backing file inside `dir` (the scratch directory, so it is
    /// deleted together with the RocksDB scratch data).
    pub fn create(dir: &Path) -> io::Result<Self> {
        let path = dir.join("flat-nodes.bin");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(INITIAL_LEN)?;
        let map = unsafe { MmapMut::map_mut(&file)? };
        Ok(FlatNodes {
            file,
            map: RwLock::new(map),
        })
    }

    /// Store a node's scaled coordinates. Safe to call concurrently from many
    /// threads as long as each id is written at most once (which the PBF
    /// format guarantees within one ingest): distinct ids are distinct slots,
    /// written with atomic stores.
    pub fn put(&self, id: i64, lon: i32, lat: i32) -> io::Result<()> {
        if !(0..=MAX_NODE_ID).contains(&id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("node id {id} out of range for --flat-nodes (0..=2^40)"),
            ));
        }
        let offset = id as u64 * SLOT_BYTES;
        let mut guard = self.map.read();
        if (guard.len() as u64) < offset + SLOT_BYTES {
            drop(guard);
            self.grow(offset + SLOT_BYTES)?;
            guard = self.map.read();
        }
        // In range per the check above; 8-aligned because the mapping is
        // page-aligned and the offset is a multiple of 8. The store is atomic,
        // so concurrent writers to *other* slots are sound.
        let slot = unsafe { &*(guard.as_ptr().add(offset as usize) as *const AtomicU64) };
        slot.store(encode(lon, lat), Ordering::Relaxed);
        Ok(())
    }

    /// Look up a node's scaled coordinates; `None` if the node was never
    /// written. Reads race-free only after all `put`s complete (the ingest's
    /// phase boundaries guarantee this ordering).
    pub fn get(&self, id: i64) -> Option<(i32, i32)> {
        if id < 0 {
            return None;
        }
        let offset = id as u64 * SLOT_BYTES;
        let guard = self.map.read();
        if (guard.len() as u64) < offset + SLOT_BYTES {
            return None;
        }
        let slot = unsafe { &*(guard.as_ptr().add(offset as usize) as *const AtomicU64) };
        decode(slot.load(Ordering::Relaxed))
    }

    fn grow(&self, needed: u64) -> io::Result<()> {
        let mut guard = self.map.write();
        // Another thread may have grown past `needed` while we waited.
        if (guard.len() as u64) >= needed {
            return Ok(());
        }
        let new_len = needed.next_power_of_two().max(INITIAL_LEN);
        self.file.set_len(new_len)?;
        *guard = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_including_zero_zero() {
        let dir = tempfile::tempdir().unwrap();
        let fnodes = FlatNodes::create(dir.path()).unwrap();
        fnodes.put(7, -1_800_000_000, 900_000_000).unwrap();
        // (0, 0) is a real coordinate and must not read back as "missing".
        fnodes.put(8, 0, 0).unwrap();
        assert_eq!(fnodes.get(7), Some((-1_800_000_000, 900_000_000)));
        assert_eq!(fnodes.get(8), Some((0, 0)));
    }

    #[test]
    fn missing_ids_are_none() {
        let dir = tempfile::tempdir().unwrap();
        let fnodes = FlatNodes::create(dir.path()).unwrap();
        fnodes.put(100, 5, -5).unwrap();
        assert_eq!(fnodes.get(99), None); // hole below a written slot
        assert_eq!(fnodes.get(1 << 30), None); // beyond current length
        assert_eq!(fnodes.get(-1), None); // negative ids are unsupported
    }

    #[test]
    fn grows_across_remap_and_keeps_old_data() {
        let dir = tempfile::tempdir().unwrap();
        let fnodes = FlatNodes::create(dir.path()).unwrap();
        fnodes.put(1, 11, 12).unwrap();
        let big_id = (INITIAL_LEN / SLOT_BYTES) as i64 * 3; // forces a grow
        fnodes.put(big_id, 21, 22).unwrap();
        assert_eq!(fnodes.get(1), Some((11, 12)));
        assert_eq!(fnodes.get(big_id), Some((21, 22)));
    }

    #[test]
    fn out_of_range_ids_error() {
        let dir = tempfile::tempdir().unwrap();
        let fnodes = FlatNodes::create(dir.path()).unwrap();
        assert!(fnodes.put(-1, 1, 1).is_err());
        assert!(fnodes.put(MAX_NODE_ID + 1, 1, 1).is_err());
    }

    #[test]
    fn concurrent_puts_spanning_growth() {
        let dir = tempfile::tempdir().unwrap();
        let fnodes = FlatNodes::create(dir.path()).unwrap();
        let slots = (INITIAL_LEN / SLOT_BYTES) as i64;
        std::thread::scope(|s| {
            for t in 0..8i64 {
                let fnodes = &fnodes;
                s.spawn(move || {
                    // Interleaved ids, some past the initial length so growth
                    // races with writes on other threads.
                    for i in 0..1000 {
                        let id = t + i * 8 + (i % 2) * slots;
                        fnodes.put(id, id as i32, -(id as i32)).unwrap();
                    }
                });
            }
        });
        for t in 0..8i64 {
            for i in 0..1000 {
                let id = t + i * 8 + (i % 2) * slots;
                assert_eq!(fnodes.get(id), Some((id as i32, -(id as i32))));
            }
        }
    }
}
