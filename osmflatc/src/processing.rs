use std::collections::BTreeMap;
use std::path::Path;

use node::storage::{NodeIdToIdxTDC, NodeIdToLonLatTDC, NodesTDC};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use tempfile::TempDir;
use way::storage::{WayIdToIdxTDC, WayIdToMbbTDC, WayTDC};

use crate::error::OsmFlatcError;

/// RocksDB column family holding the encoded relations, keyed by spatial order.
pub const RELATIONS: &str = "relations";
/// RocksDB column family holding the relations' string references, in the same
/// order as [`RELATIONS`].
pub const RELATIONS_STRING_REFS: &str = "relations_string_refs";

#[cfg(test)]
mod mock;
pub mod node;
pub(crate) mod storage;
pub mod way;

pub(crate) trait Key: From<Box<[u8]>> {
    fn serialize(&self) -> Vec<u8>;
}

pub(crate) trait Value: From<Box<[u8]>> {
    fn serialize(&self) -> Vec<u8>;
}

pub(crate) trait TempDataCodec {
    type Key: Key;
    type Value: Value;
    const NAME: &'static str;
}

pub trait RocksDBUnsync {
    fn put<TDC: TempDataCodec>(&mut self, key: TDC::Key, value: TDC::Value);
}

pub trait RocksDBSync {
    fn get<TDC: TempDataCodec>(&self, key: &TDC::Key) -> Result<Option<TDC::Value>, OsmFlatcError>;
}

#[derive(Default)]
pub struct WriteBatchInternal<'a> {
    batch: WriteBatch,
    families: BTreeMap<String, &'a ColumnFamily>,
}

impl<'a> WriteBatchInternal<'a> {
    pub fn inner(self) -> WriteBatch {
        self.batch
    }

    pub fn insert_cf(&mut self, name: &str, cf: &'a ColumnFamily) {
        self.families.insert(name.to_owned(), cf);
    }
}

impl<'a> RocksDBUnsync for WriteBatchInternal<'a> {
    fn put<TDC: TempDataCodec>(&mut self, key: TDC::Key, value: TDC::Value) {
        let fam = self.families.get(TDC::NAME).unwrap();
        self.batch.put_cf(fam, key.serialize(), value.serialize());
    }
}

pub(crate) trait RocksDB {
    #[allow(clippy::type_complexity)]
    fn iterator<'a, TDC: TempDataCodec>(
        &'a self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(TDC::Key, TDC::Value), OsmFlatcError>> + 'a>,
        OsmFlatcError,
    >
    where
        <TDC as TempDataCodec>::Key: 'a,
        <TDC as TempDataCodec>::Value: 'a;
}

impl RocksDBSync for DB {
    fn get<TDC: TempDataCodec>(&self, key: &TDC::Key) -> Result<Option<TDC::Value>, OsmFlatcError> {
        let cf = self.cf_handle(TDC::NAME).unwrap();

        self.get_cf(cf, key.serialize())
            .map_err(OsmFlatcError::RocksDB)
            .map(|v| v.map(|b| TDC::Value::from(b.into())))
    }
}

impl RocksDB for DB {
    fn iterator<'a, TDC: TempDataCodec>(
        &'a self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(TDC::Key, TDC::Value), OsmFlatcError>> + 'a>,
        OsmFlatcError,
    >
    where
        <TDC as TempDataCodec>::Key: 'a,
        <TDC as TempDataCodec>::Value: 'a,
    {
        let cf = self.cf_handle(TDC::NAME).unwrap();

        let iter = self.iterator_cf(cf, IteratorMode::Start);
        Ok(Box::new(iter.map(|res| {
            res.map_err(OsmFlatcError::RocksDB)
                .map(|(k, v)| (TDC::Key::from(k), TDC::Value::from(v)))
        })))
    }
}

/// Open the temporary RocksDB used to sort entities into spatial order.
///
/// The database lives in a freshly created temporary directory under
/// `scratch_parent` (kept on the same filesystem as the output so the
/// potentially large scratch data does not spill onto a small `/tmp`). The
/// returned [`TempDir`] owns that directory and removes it on drop, so callers
/// must keep it alive at least as long as the returned [`DB`].
pub fn create_db(scratch_parent: &Path) -> Result<(DB, TempDir), Box<dyn std::error::Error>> {
    let scratch = tempfile::Builder::new()
        .prefix(".osmflatc-scratch-")
        .tempdir_in(scratch_parent)?;

    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);

    let cfs = [
        NodesTDC::NAME,
        NodeIdToLonLatTDC::NAME,
        NodeIdToIdxTDC::NAME,
        WayTDC::NAME,
        WayIdToMbbTDC::NAME,
        WayIdToIdxTDC::NAME,
        RELATIONS,
        RELATIONS_STRING_REFS,
    ]
    .iter()
    .map(move |v| ColumnFamilyDescriptor::new(v.to_string(), cf_opts.clone()));

    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    db_opts.increase_parallelism(16);
    db_opts.set_write_buffer_size(512 * 1024 * 1024);

    let db = DB::open_cf_descriptors(&db_opts, scratch.path(), cfs)?;
    Ok((db, scratch))
}
