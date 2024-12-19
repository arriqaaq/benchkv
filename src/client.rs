use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::workload::LoadPattern;

#[async_trait]
pub trait Client: Send + Sync + 'static {
    /// Load initial dataset into the database
    async fn load_initial_dataset(
        &self,
        record_count: u32,
        load_pattern: LoadPattern,
    ) -> Result<()>;

    /// Read a single record by key
    async fn read(&self, key: &str) -> Result<Option<Value>>;

    /// Insert a new record
    async fn insert(&self, key: &str, value: Value) -> Result<()>;

    /// Update an existing record
    async fn update(&self, key: &str, value: Value) -> Result<()>;

    /// Scan records starting from a key
    async fn scan(
        &self,
        start_key: &str,
        record_count: Option<usize>,
    ) -> Result<Vec<(String, Value)>>;

    /// Read and update a record in one operation
    async fn read_modify_write(&self, key: &str, value: Value) -> Result<()>;

    /// Delete a record
    async fn delete(&self, key: &str) -> Result<()>;
}
