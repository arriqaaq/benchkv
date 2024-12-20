#![cfg(feature = "rocksdb")]

use crate::client::Client;
use crate::workload::LoadPattern;
use anyhow::Result;
use async_trait::async_trait;
use rocksdb::{
    DBCompactionStyle, DBCompressionType, Direction, IteratorMode, LogLevel,
    Options as RocksDBOptions, ReadOptions, Transaction, TransactionDB, TransactionDBOptions,
    TransactionOptions, WriteOptions,
};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;

const DB_PATH: &str = "rocksdb_benchmark";

#[derive(Clone)]
pub struct RocksDBClient {
    pub(crate) db: Arc<TransactionDB>,
    db_path: PathBuf,
}

impl RocksDBClient {
    pub fn new() -> Result<Self> {
        let db_path = PathBuf::from(DB_PATH);
        // Clean up any existing database directory
        if db_path.exists() {
            std::fs::remove_dir_all(&db_path)?;
        }

        // Configure custom options
        let mut opts = RocksDBOptions::default();
        // Ensure we use fdatasync
        opts.set_use_fsync(false);
        // Only use warning log level
        opts.set_log_level(LogLevel::Error);
        // Set the number of log files to keep
        opts.set_keep_log_file_num(20);
        // Create database if missing
        opts.create_if_missing(true);
        // Create column families if missing
        opts.create_missing_column_families(true);
        // Set the datastore compaction style
        opts.set_compaction_style(DBCompactionStyle::Level);
        // Increase the background thread count
        opts.increase_parallelism(8);
        // Set the maximum number of write buffers
        opts.set_max_write_buffer_number(32);
        // Set the amount of data to build up in memory
        opts.set_write_buffer_size(256 * 1024 * 1024);
        // Set the target file size for compaction
        opts.set_target_file_size_base(512 * 1024 * 1024);
        // Set minimum number of write buffers to merge
        opts.set_min_write_buffer_number_to_merge(4);
        // Use separate write thread queues
        opts.set_enable_pipelined_write(true);
        // Enable separation of keys and values
        opts.set_enable_blob_files(true);
        // Store 4KB values separate from keys
        opts.set_min_blob_size(4 * 1024);
        // Set specific compression levels
        opts.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Snappy,
            DBCompressionType::Snappy,
            DBCompressionType::Snappy,
        ]);

        let txn_db_opts = TransactionDBOptions::default();
        let db = TransactionDB::open(&opts, &txn_db_opts, &db_path)?;

        Ok(Self {
            db: Arc::new(db),
            db_path,
        })
    }

    fn get_transaction(&self) -> Transaction<TransactionDB> {
        // Set the transaction options
        let mut txn_opts = TransactionOptions::default();
        txn_opts.set_snapshot(true);

        // Set the write options
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false); // Better performance

        // Create transaction with options
        self.db.transaction_opt(&write_opts, &txn_opts)
    }

    fn cleanup(&self) -> std::io::Result<()> {
        if self.db_path.exists() {
            std::fs::remove_dir_all(&self.db_path)?;
        }
        Ok(())
    }
}

impl Drop for RocksDBClient {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            eprintln!("Error cleaning up database directory: {}", e);
        }
    }
}

impl RocksDBClient {
    async fn load_initial_dataset_sequential(&self, record_count: u32) -> Result<()> {
        let txn = self.get_transaction();

        for i in 0..record_count {
            let key = format!("user{}", i);
            let value = serde_json::json!({
                "field1": i,
                "field2": format!("initial_value{}", i),
                "field3": vec![1, 2, 3, 4, 5],
            });
            let serialized = serde_json::to_vec(&value)?;
            txn.put(key.as_bytes(), &serialized)?;
        }

        txn.commit()?;
        Ok(())
    }

    async fn load_initial_dataset_random(&self, record_count: u32) -> Result<()> {
        let txn = self.get_transaction();

        // Create a vector of indices and shuffle it
        let mut indices: Vec<u32> = (0..record_count).collect();
        use rand::seq::SliceRandom;
        indices.shuffle(&mut rand::thread_rng());

        for i in indices {
            let key = format!("user{}", i);
            let value = serde_json::json!({
                "field1": i,
                "field2": format!("initial_value{}", i),
                "field3": vec![1, 2, 3, 4, 5],
            });
            let serialized = serde_json::to_vec(&value)?;
            txn.put(key.as_bytes(), &serialized)?;
        }

        txn.commit()?;
        Ok(())
    }

    // Simpler read implementation that doesn't use transactions
    async fn read(&self, key: &str) -> Result<Option<Value>> {
        let mut read_opts = ReadOptions::default();
        read_opts.set_async_io(true);
        read_opts.fill_cache(true);

        if let Some(data) = self.db.get_opt(key.as_bytes(), &read_opts)? {
            let value: Value = serde_json::from_slice(&data)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl Client for RocksDBClient {
    async fn load_initial_dataset(
        &self,
        record_count: u32,
        load_pattern: LoadPattern,
    ) -> Result<()> {
        match load_pattern {
            LoadPattern::Sequential => self.load_initial_dataset_sequential(record_count).await,
            LoadPattern::Random => self.load_initial_dataset_random(record_count).await,
        }
    }

    async fn read(&self, key: &str) -> Result<Option<Value>> {
        self.read(key).await
    }

    async fn insert(&self, key: &str, value: Value) -> Result<()> {
        let txn = self.get_transaction();
        let serialized = serde_json::to_vec(&value)?;
        txn.put(key.as_bytes(), &serialized)?;
        txn.commit()?;
        Ok(())
    }

    async fn update(&self, key: &str, value: Value) -> Result<()> {
        self.insert(key, value).await
    }

    async fn scan(
        &self,
        start_key: &str,
        record_count: Option<usize>,
    ) -> Result<Vec<(String, Value)>> {
        let mut results = Vec::new();
        let mut opts = ReadOptions::default();
        opts.set_async_io(true);
        opts.fill_cache(true);

        let iter = self.db.iterator_opt(
            IteratorMode::From(start_key.as_bytes(), Direction::Forward),
            opts,
        );

        for (i, item) in iter.enumerate() {
            let (key, value) = item?;
            let key_str = String::from_utf8(key.to_vec())?;
            let value: Value = serde_json::from_slice(&value)?;
            results.push((key_str, value));

            if let Some(limit) = record_count {
                if i >= limit - 1 {
                    break;
                }
            }
        }

        Ok(results)
    }

    async fn read_modify_write(&self, key: &str, value: Value) -> Result<()> {
        // Create transaction
        let txn = self.db.transaction();

        // Read with transaction
        let _existing = txn.get(key.as_bytes())?;

        // Write directly with transaction
        let serialized = serde_json::to_vec(&value)?;
        txn.put(key.as_bytes(), &serialized)?;

        // Commit the transaction
        txn.commit()?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let txn = self.get_transaction();
        txn.delete(key.as_bytes())?;
        txn.commit()?;
        Ok(())
    }
}
