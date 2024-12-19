use crate::{client::Client, workload::LoadPattern};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::path::PathBuf;
#[cfg(feature = "surrealkv")]
use std::sync::Arc;

const DB_PATH: &str = "surrealkv_ycsb";

#[cfg(feature = "surrealkv")]
#[derive(Clone)]
pub struct SurrealKVClient {
    pub(crate) db: Arc<surrealkv::Store>,
    db_path: PathBuf,
}

impl SurrealKVClient {
    pub fn new() -> Result<Self> {
        let db_path = PathBuf::from(DB_PATH);
        // Clean up any existing database directory
        if db_path.exists() {
            std::fs::remove_dir_all(&db_path)?;
        }

        let mut opts = surrealkv::Options::new();
        opts.enable_versions = false;
        opts.disk_persistence = true;
        opts.dir = db_path.clone();

        let store = Arc::new(surrealkv::Store::new(opts)?);

        Ok(Self { db: store, db_path })
    }

    fn cleanup(&self) -> std::io::Result<()> {
        if self.db_path.exists() {
            std::fs::remove_dir_all(&self.db_path)?;
        }
        Ok(())
    }
}

impl Drop for SurrealKVClient {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            eprintln!("Error cleaning up database directory: {}", e);
        }
    }
}

#[cfg(feature = "surrealkv")]
impl SurrealKVClient {
    async fn load_initial_dataset_sequential(&self, record_count: u32) -> Result<()> {
        let mut txn = self.db.begin_with_mode(surrealkv::Mode::ReadWrite)?;

        for i in 0..record_count {
            let key = format!("user{}", i);
            let value = serde_json::json!({
            "field1": i,
            "field2": format!("initial_value{}", i),
            "field3": vec![1, 2, 3, 4, 5],
            });
            let serialized = serde_json::to_vec(&value)?;
            txn.set(key.as_bytes(), &serialized)?;
        }

        txn.commit().await?;
        Ok(())
    }

    async fn load_initial_dataset_random(&self, record_count: u32) -> Result<()> {
        let mut txn = self.db.begin_with_mode(surrealkv::Mode::ReadWrite)?;

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
            txn.set(key.as_bytes(), &serialized)?;
        }

        txn.commit().await?;
        Ok(())
    }
}

#[cfg(feature = "surrealkv")]
#[async_trait]
impl Client for SurrealKVClient {
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
        let mut txn = self.db.begin_with_mode(surrealkv::Mode::ReadOnly)?;

        if let Some(data) = txn.get(key.as_bytes())? {
            let value: Value = serde_json::from_slice(&data)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    async fn insert(&self, key: &str, value: Value) -> Result<()> {
        let mut txn = self.db.begin_with_mode(surrealkv::Mode::ReadWrite)?;
        let serialized = serde_json::to_vec(&value)?;
        txn.set(key.as_bytes(), &serialized)?;
        txn.commit().await?;
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
        let mut txn = self.db.begin_with_mode(surrealkv::Mode::ReadOnly)?;
        let mut results = Vec::new();

        let range = start_key.as_bytes()..;
        for (key, value, _) in txn.scan(range, record_count)? {
            let key_str = String::from_utf8(key.to_vec())?;
            let value: Value = serde_json::from_slice(&value)?;
            results.push((key_str, value));
        }

        Ok(results)
    }

    async fn read_modify_write(&self, key: &str, value: Value) -> Result<()> {
        let mut txn = self.db.begin_with_mode(surrealkv::Mode::ReadWrite)?;

        // Read
        let _existing = txn.get(key.as_bytes())?;

        // Write
        let serialized = serde_json::to_vec(&value)?;
        txn.set(key.as_bytes(), &serialized)?;

        txn.commit().await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut txn = self.db.begin_with_mode(surrealkv::Mode::ReadWrite)?;
        txn.delete(key.as_bytes())?;
        txn.commit().await?;
        Ok(())
    }
}
