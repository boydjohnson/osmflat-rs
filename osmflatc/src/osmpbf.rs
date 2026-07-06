#![allow(unknown_lints, clippy::derive_partial_eq_without_eq)]

use byteorder::{ByteOrder, NetworkEndian};
use flate2::read::ZlibDecoder;
use log::info;
use prost::{self, Message};
use rayon::prelude::*;

use std::io::{self, Read};

include!(concat!(env!("OUT_DIR"), "/osmpbf.rs"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlockType {
    Header,
    Nodes,
    DenseNodes,
    Ways,
    Relations,
}

/// Decode block type from PrimitiveBlock protobuf message
///
/// This does not decode any fields, it just checks which tags are present
/// in PrimitiveGroup fields of the message.
///
/// `blob` should contain decompressed data of an OSMData PrimitiveBlock.
///
/// Note: We use public API of `prost` crate, which though is not exposed in
/// the crate and marked with comment that it should be only used from
/// `prost::Message`.
pub fn type_and_granularity_from_osmdata_blob(mut blob: &[u8]) -> io::Result<(BlockType, u64)> {
    const PRIMITIVE_GROUP_TAG: u32 = 2;
    const GRANULARITY_TAG: u32 = 17;
    const NODES_TAG: u32 = 1;
    const DENSE_NODES_TAG: u32 = 2;
    const WAY_STAG: u32 = 3;
    const RELATIONS_TAG: u32 = 4;
    const CHANGESETS_TAG: u32 = 5;

    let mut block_type = None;
    let mut granularity = 100; // default value
    while !blob.is_empty() {
        // decode fields of PrimitiveBlock
        let (key, wire_type) = prost::encoding::decode_key(&mut blob)?;
        let mut blob_copy = blob;
        if key == PRIMITIVE_GROUP_TAG {
            // We found a PrimitiveGroup field. There could be several of them, but
            // follwoing the specs of OSMPBF, all of them will have the same single
            // optional field, which defines the type of the block.

            // Decode the number of primitive groups.
            let _ = prost::encoding::decode_varint(&mut blob_copy)?;
            // Decode the tag of the first primitive group defining the type.
            let (tag, _wire_type) = prost::encoding::decode_key(&mut blob_copy)?;
            block_type = match tag {
                NODES_TAG => Some(BlockType::Nodes),
                DENSE_NODES_TAG => Some(BlockType::DenseNodes),
                WAY_STAG => Some(BlockType::Ways),
                RELATIONS_TAG => Some(BlockType::Relations),
                CHANGESETS_TAG => {
                    panic!("found block containing unsupported changesets");
                }
                _ => {
                    panic!("invalid input data: malformed primitive block");
                }
            };
        } else if key == GRANULARITY_TAG {
            granularity = prost::encoding::decode_varint(&mut blob_copy)?;
        }
        // skip payload
        prost::encoding::skip_field(
            wire_type,
            key,
            &mut blob,
            prost::encoding::DecodeContext::default(),
        )?;
    }
    match block_type {
        None => panic!("Found block without primitive group"),
        Some(x) => Ok((x, granularity)),
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct BlockIndex {
    pub block_type: BlockType,
    pub granularity: Option<u64>,
    pub blob_start: usize,
    pub blob_len: usize,
}

struct BlockIndexIterator<'a> {
    data: &'a [u8],
    cursor: usize,
}

/// Framing of one blob: its `BlobHeader` type plus the payload location.
/// Produced by the cheap sequential framing walk; the payload itself is not
/// touched until the parallel classification pass.
enum BlobInfo {
    Header(BlockIndex),
    Unknown(usize, usize),
}

impl<'a> BlockIndexIterator<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, cursor: 0 }
    }

    fn read(&mut self, len: usize) -> &[u8] {
        let data = &self.data[self.cursor..self.cursor + len];
        self.cursor += len;
        data
    }

    fn next_blob(&mut self) -> Result<BlobInfo, io::Error> {
        // read size of blob header
        let blob_header_len: i32 = NetworkEndian::read_i32(self.read(4));

        // read blob header
        let blob_header = BlobHeader::decode(self.read(blob_header_len as usize))?;

        let blob_start = self.cursor;
        let blob_len = blob_header.datasize as usize;
        // Skip the payload -- only the framing is read in this pass.
        self.cursor += blob_len;

        if blob_header.r#type == "OSMHeader" {
            Ok(BlobInfo::Header(BlockIndex {
                block_type: BlockType::Header,
                granularity: None,
                blob_start,
                blob_len,
            }))
        } else if blob_header.r#type == "OSMData" {
            Ok(BlobInfo::Unknown(blob_start, blob_len))
        } else {
            panic!("unknown blob type");
        }
    }
}

impl<'a> Iterator for BlockIndexIterator<'a> {
    type Item = Result<BlobInfo, io::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.data.len() {
            Some(self.next_blob())
        } else {
            None
        }
    }
}

pub fn read_block<T: prost::Message + Default>(
    data: &[u8],
    idx: &BlockIndex,
) -> Result<T, io::Error> {
    let blob = Blob::decode(&data[idx.blob_start..idx.blob_start + idx.blob_len])?;

    let mut blob_buf = Vec::with_capacity(blob.raw_size.unwrap_or(0) as usize);
    let blob_data = if blob.raw.is_some() {
        blob.raw.as_ref().unwrap()
    } else if blob.zlib_data.is_some() {
        // decompress zlib data
        let data: &Vec<u8> = blob.zlib_data.as_ref().unwrap();
        let mut decoder = ZlibDecoder::new(&data[..]);
        decoder.read_to_end(&mut blob_buf)?;
        &blob_buf
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown compression",
        ));
    };
    Ok(T::decode(blob_data.as_slice())?)
}

fn blob_type_and_granularity_from_blob_info(
    blob_start: usize,
    blob_len: usize,
    blob: &[u8],
) -> Result<BlockIndex, io::Error> {
    let blob = Blob::decode(blob)?;

    // `raw_size` is the exact decompressed length; reserving it up front
    // avoids several realloc+copy rounds per blob in `read_to_end`.
    let mut blob_buf = Vec::with_capacity(blob.raw_size.unwrap_or(0) as usize);
    let blob_data = if blob.raw.is_some() {
        // use raw bytes
        blob.raw.as_ref().unwrap()
    } else if blob.zlib_data.is_some() {
        // decompress zlib data
        let data: &Vec<u8> = blob.zlib_data.as_ref().unwrap();
        let mut decoder = ZlibDecoder::new(&data[..]);
        decoder.read_to_end(&mut blob_buf).map_err(|e| {
            // A valid zlib stream starts 0x78; anything else at this offset
            // means bad framing or a corrupt file rather than decoder trouble.
            let head: Vec<String> = data.iter().take(4).map(|b| format!("{b:02x}")).collect();
            io::Error::new(
                e.kind(),
                format!(
                    "{e} (blob at file offset {blob_start}, len {blob_len}, \
                     zlib_data len {}, first bytes [{}])",
                    data.len(),
                    head.join(" ")
                ),
            )
        })?;
        &blob_buf
    } else {
        panic!("can only read raw or zlib compressed blob");
    };
    assert_eq!(
        blob_data.len(),
        blob.raw_size.unwrap_or(blob_data.len() as i32) as usize
    );

    let (block_type, granularity) = type_and_granularity_from_osmdata_blob(&blob_data[..])?;
    Ok(BlockIndex {
        block_type,
        granularity: Some(granularity),
        blob_start,
        blob_len,
    })
}

pub fn build_block_index(pbf_data: &[u8]) -> Vec<BlockIndex> {
    // Classifying a blob requires inflating it (type and granularity live
    // inside the compressed PrimitiveBlock), so on a planet file this pass
    // decompresses the entire input. Split it in two so that cost
    // parallelizes: a sequential framing walk that reads only blob headers
    // (never payloads), then an indexed parallel pass in which every worker
    // inflates blobs straight from the input mmap -- no single-producer
    // copy bottleneck.
    let framing: Vec<BlobInfo> = BlockIndexIterator::new(pbf_data)
        .filter_map(|blob| match blob {
            Ok(info) => Some(info),
            Err(e) => {
                eprintln!("Skipping block due to error: {e}");
                None
            }
        })
        .collect();

    let mut result: Vec<BlockIndex> = framing
        .into_par_iter()
        .filter_map(|info| {
            let block = match info {
                BlobInfo::Header(b) => Ok(b),
                BlobInfo::Unknown(start, len) => blob_type_and_granularity_from_blob_info(
                    start,
                    len,
                    &pbf_data[start..start + len],
                ),
            };
            match block {
                Ok(b) => Some(b),
                Err(e) => {
                    eprintln!("Skipping block due to error: {e}");
                    None
                }
            }
        })
        .collect();
    result.par_sort_unstable();
    info!("Found {} blocks", result.len());
    result
}
