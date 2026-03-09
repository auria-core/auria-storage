// File: storage_interface.rs - This file is part of AURIA
// Copyright (c) 2026 AURIA Developers and Contributors
// Description:
//     Unified storage interface for AURIA Runtime Core.
//     Provides a consistent API across different storage backends.

use auria_core::{AuriaError, AuriaResult, Shard, ShardId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    Redis,
    S3,
    SQLite,
    PostgreSQL,
    File,
    Memory,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub redis: Option<RedisStorageConfig>,
    pub s3: Option<S3StorageConfig>,
    pub sql: Option<SqlStorageConfig>,
    pub file: Option<FileStorageConfig>,
    pub memory: Option<MemoryStorageConfig>,
    pub cache_size: usize,
    pub ttl: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct RedisStorageConfig {
    pub url: String,
    pub key_prefix: String,
    pub default_ttl_seconds: Option<u64>,
    pub db: Option<u8>,
}

#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub use_path_style: bool,
}

#[derive(Debug, Clone)]
pub struct SqlStorageConfig {
    pub database_url: String,
    pub table_name: String,
}

#[derive(Debug, Clone)]
pub struct FileStorageConfig {
    pub root_path: PathBuf,
    pub max_size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryStorageConfig {
    pub max_items: usize,
}

#[derive(Debug, Clone)]
pub struct StorageStats {
    pub backend: StorageBackend,
    pub total_shards: u64,
    pub cache_size: usize,
    pub cache_capacity: usize,
    pub last_error: Option<String>,
}

#[async_trait::async_trait]
pub trait StorageBackendTrait: Send + Sync {
    async fn get_shard(&self, shard_id: ShardId) -> AuriaResult<Shard>;
    async fn put_shard(&self, shard: Shard) -> AuriaResult<()>;
    async fn delete_shard(&self, shard_id: ShardId) -> AuriaResult<()>;
    async fn exists(&self, shard_id: ShardId) -> bool;
    async fn list_shards(&self, limit: Option<usize>) -> AuriaResult<Vec<ShardId>>;
    async fn get_stats(&self) -> StorageStats;
    async fn clear(&self) -> AuriaResult<()>;
}

pub struct Storage {
    backend: Arc<dyn StorageBackendTrait>,
    cache: Arc<RwLock<lru::LruCache<ShardId, Shard>>>,
    config: StorageConfig,
}

impl Storage {
    pub async fn new(config: StorageConfig) -> AuriaResult<Self> {
        let backend = Self::create_backend(config.clone()).await?;
        let cache = Arc::new(RwLock::new(
            lru::LruCache::new(
                NonZeroUsize::new(config.cache_size.max(1)).unwrap()
            )
        ));

        Ok(Self { backend: Arc::new(backend), cache, config })
    }

    async fn create_backend(config: StorageConfig) -> AuriaResult<Box<dyn StorageBackendTrait>> {
        match config.backend {
            StorageBackend::Redis = > {
                let redis_config = config.redis.clone().unwrap_or_else(||
                    RedisStorageConfig {
                        url: "redis://localhost:6379".to_string(),
                        key_prefix: "auria:shard".to_string(),
                        default_ttl_seconds: Some(3600),
                        db: Some(0),
                    }
                );
                Ok(Box::new(RedisStorage::new(redis_config, config.cache_size)))
            }
            StorageBackend::S3 = > {
                let s3_config = config.s3.clone().unwrap_or_else(||
                    S3StorageConfig {
                        bucket: "auria-shards".to_string(),
                        region: "us-east-1".to_string(),
                        endpoint: None,
                        access_key: "".to_string(),
                        secret_key: "".to_string(),
                        use_path_style: false,
                    }
                );
                Ok(Box::new(S3Storage::new(s3_config, config.cache_size)))
            }
            StorageBackend::SQLite = > {
                let sql_config = config.sql.clone().unwrap_or_else(||
                    SqlStorageConfig::sqlite(PathBuf::from("/data/auria.db"))
                );
                Ok(Box::new(SqlStorage::new(sql_config, config.cache_size)))
            }
            StorageBackend::PostgreSQL = > {
                let sql_config = config.sql.clone().unwrap_or_else(||
                    SqlStorageConfig::new("postgres://localhost/auria".to_string())
                );
                Ok(Box::new(SqlStorage::new(sql_config, config.cache_size)))
            }
            StorageBackend::File = > {
                let file_config = config.file.clone().unwrap_or_else(||
                    FileStorageConfig {
                        root_path: PathBuf::from("/data/shards"),
                        max_size_bytes: 10 * 1024 * 1024 * 1024, // 10GB
                    }
                );
                Ok(Box::new(FileStorage::new(file_config, config.cache_size)))
            }
            StorageBackend::Memory = > {
                let memory_config = config.memory.clone().unwrap_or_else(||
                    MemoryStorageConfig { max_items: 1000 }
                );
                Ok(Box::new(MemoryStorage::new(memory_config.max_items)))
            }
        }
    }

    pub async fn get_shard(&self, shard_id: ShardId) -> AuriaResult<Shard> {
        // Check local cache first
        {
            let mut cache = self.cache.write().await;
            if let Some(shard) = cache.get(&shard_id) {
                return Ok(shard.clone());
            }
        }

        // Fetch from backend
        let shard = self.backend.get_shard(shard_id).await?;

        // Update local cache
        {
            let mut cache = self.cache.write().await;
            if cache.len() >= self.config.cache_size {
                cache.pop_lru();
            }
            cache.put(shard_id, shard.clone());
        }

        Ok(shard)
    }

    pub async fn put_shard(&self, shard: Shard) -> AuriaResult<()> {
        // Store in backend
        self.backend.put_shard(shard.clone()).await?;

        // Update local cache
        {
            let mut cache = self.cache.write().await;
            if cache.len() >= self.config.cache_size {
                cache.pop_lru();
            }
            cache.put(shard.id, shard);
        }

        Ok(())
    }

    pub async fn delete_shard(&self, shard_id: ShardId) -> AuriaResult<()> {
        // Delete from backend
        self.backend.delete_shard(shard_id).await?;

        // Remove from local cache
        {
            let mut cache = self.cache.write().await;
            cache.pop(&shard_id);
        }

        Ok(())
    }

    pub async fn exists(&self, shard_id: ShardId) -> bool {
        // Check local cache first
        {
            let cache = self.cache.read().await;
            if cache.contains(&shard_id) {
                return true;
            }
        }

        // Check backend
        self.backend.exists(shard_id).await
    }

    pub async fn list_shards(&self, limit: Option<usize>) -> AuriaResult<Vec<ShardId>> {
        self.backend.list_shards(limit).await
    }

    pub async fn get_stats(&self) -> StorageStats {
        self.backend.get_stats().await
    }

    pub async fn clear(&self) -> AuriaResult<()> {
        // Clear local cache
        {
            let mut cache = self.cache.write().await;
            cache.clear();
        }

        // Clear backend
        self.backend.clear().await
    }
}

// File storage implementation
pub struct FileStorage {
    config: FileStorageConfig,
    cache: Arc<RwLock<lru::LruCache<ShardId, Shard>>>,
    max_cache_size: usize,
}

impl FileStorage {
    pub fn new(config: FileStorageConfig, cache_size: usize) -> Self {
        let cache = lru::LruCache::new(
            NonZeroUsize::new(cache_size.max(1)).unwrap()
        );
        Self {
            config,
            cache: Arc::new(RwLock::new(cache)),
            max_cache_size: cache_size,
        }
    }
}

#[async_trait::async_trait]
impl StorageBackendTrait for FileStorage {
    async fn get_shard(&self, shard_id: ShardId) -> AuriaResult<Shard> {
        {
            let mut cache = self.cache.write().await;
            if let Some(shard) = cache.get(&shard_id) {
                return Ok(shard.clone());
            }
        }

        let path = self.config.root_path.join(hex::encode(shard_id.0));

        if !path.exists() {
            return Err(AuriaError::ShardNotFound(shard_id));
        }

        let data = tokio::fs::read(path).await
            .map_err(|e| AuriaError::StorageError(format!("Failed to read shard from file: {}", e)))?;

        let shard: Shard = serde_json::from_slice(&data)
            .map_err(|e| AuriaError::SerializationError(format!("Failed to deserialize shard: {}", e)))?;

        {
            let mut cache = self.cache.write().await;
            if cache.len() >= self.max_cache_size {
                cache.pop_lru();
            }
            cache.put(shard_id, shard.clone());
        }

        Ok(shard)
    }

    async fn put_shard(&self, shard: Shard) -> AuriaResult<()> {
        let path = self.config.root_path.join(hex::encode(shard.id.0));

        tokio::fs::create_dir_all(&self.config.root_path).await.ok();

        let data = serde_json::to_vec(&shard)
            .map_err(|e| AuriaError::SerializationError(format!("Failed to serialize shard: {}", e)))?;

        tokio::fs::write(&path, data).await
            .map_err(|e| AuriaError::StorageError(format!("Failed to write shard to file: {}", e)))?;

        {
            let mut cache = self.cache.write().await;
            if cache.len() >= self.max_cache_size {
                cache.pop_lru();
            }
            cache.put(shard.id, shard);
        }

        Ok(())
    }

    async fn delete_shard(&self, shard_id: ShardId) -> AuriaResult<()> {
        let path = self.config.root_path.join(hex::encode(shard_id.0));

        if path.exists() {
            tokio::fs::remove_file(path).await
                .map_err(|e| AuriaError::StorageError(format!("Failed to delete shard file: {}", e)))?;
        }

        {
            let mut cache = self.cache.write().await;
            cache.pop(&shard_id);
        }

        Ok(())
    }

    async fn exists(&self, shard_id: ShardId) -> bool {
        if self.cache.read().await.contains(&shard_id) {
            return true;
        }

        let path = self.config.root_path.join(hex::encode(shard_id.0));
        path.exists()
    }

    async fn list_shards(&self, limit: Option<usize>) -> AuriaResult<Vec<ShardId>> {
        let mut shard_ids = Vec::new();

        if let Ok(entries) = tokio::fs::read_dir(&self.config.root_path).await {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if file_name.ends_with(".json") {
                            let id_str = file_name.strip_suffix(".json").unwrap_or(file_name);
                            if let Ok(id_bytes) = hex::decode(id_str) {
                                if id_bytes.len() == 32 {
                                    shard_ids.push(ShardId(id_bytes));
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(limit) = limit {
            shard_ids.truncate(limit);
        }

        Ok(shard_ids)
    }

    async fn get_stats(&self) -> StorageStats {
        let cache = self.cache.read().await;
        StorageStats {
            backend: StorageBackend::File,
            total_shards: 0, // File storage doesn't track total count
            cache_size: cache.len(),
            cache_capacity: self.max_cache_size,
            last_error: None,
        }
    }

    async fn clear(&self) -> AuriaResult<()> {
        {
            let mut cache = self.cache.write().await;
            cache.clear();
        }

        if self.config.root_path.exists() {
            tokio::fs::remove_dir_all(&self.config.root_path).await
                .map_err(|e| AuriaError::StorageError(format!("Failed to clear file storage: {}", e)))?;
        }

        Ok(())
    }
}

// Memory storage implementation
pub struct MemoryStorage {
    store: Arc<RwLock<HashMap<ShardId, Shard>>>,
    max_items: usize,
}

impl MemoryStorage {
    pub fn new(max_items: usize) -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            max_items,
        }
    }
}

#[async_trait::async_trait]
impl StorageBackendTrait for MemoryStorage {
    async fn get_shard(&self, shard_id: ShardId) -> AuriaResult<Shard> {
        let store = self.store.read().await;
        if let Some(shard) = store.get(&shard_id) {
            return Ok(shard.clone());
        }
        Err(AuriaError::ShardNotFound(shard_id))
    }

    async fn put_shard(&self, shard: Shard) -> AuriaResult<()> {
        let mut store = self.store.write().await;
        if store.len() >= self.max_items {
            // Simple eviction policy - remove random item
            if let Some((key, _)) = store.iter().next() {
                store.remove(key);
            }
        }
        store.insert(shard.id, shard);
        Ok(())
    }

    async fn delete_shard(&self, shard_id: ShardId) -> AuriaResult<()> {
        let mut store = self.store.write().await;
        store.remove(&shard_id);
        Ok(())
    }

    async fn exists(&self, shard_id: ShardId) -> bool {
        self.store.read().await.contains_key(&shard_id)
    }

    async fn list_shards(&self, limit: Option<usize>) -> AuriaResult<Vec<ShardId>> {
        let store = self.store.read().await;
        let mut shard_ids: Vec<ShardId> = store.keys().cloned().collect();

        if let Some(limit) = limit {
            shard_ids.truncate(limit);
        }

        Ok(shard_ids)
    }

    async fn get_stats(&self) -> StorageStats {
        let store = self.store.read().await;
        StorageStats {
            backend: StorageBackend::Memory,
            total_shards: store.len() as u64,
            cache_size: store.len(),
            cache_capacity: self.max_items,
            last_error: None,
        }
    }

    async fn clear(&self) -> AuriaResult<()> {
        let mut store = self.store.write().await;
        store.clear();
        Ok(())
    }
}

// Enhanced ModelStore with storage integration
pub struct EnhancedModelStore {
    model_store: ModelStore,
    storage: Storage,
}

impl EnhancedModelStore {
    pub async fn new(storage_config: StorageConfig) -> AuriaResult<Self> {
        let storage = Storage::new(storage_config).await?;
        Ok(Self {
            model_store: ModelStore::new(),
            storage,
        })
    }

    pub async fn load_shard(&self, shard_id: ShardId) -> AuriaResult<Shard> {
        if self.model_store.shard_exists(&shard_id) {
            // Shard exists in model store, load from storage
            self.storage.get_shard(shard_id).await
        } else {
            Err(AuriaError::ShardNotFound(shard_id))
        }
    }

    pub async fn store_shard(&self, shard: Shard) -> AuriaResult<()> {
        // Store in model store
        self.model_store.store_shard(shard.clone());

        // Store in storage backend
        self.storage.put_shard(shard).await
    }

    pub async fn shard_exists(&self, shard_id: ShardId) -> bool {
        self.model_store.shard_exists(&shard_id) && self.storage.exists(shard_id).await
    }

    pub async fn list_shards(&self, limit: Option<usize>) -> AuriaResult<Vec<ShardId>> {
        self.storage.list_shards(limit).await
    }

    pub async fn get_storage_stats(&self) -> StorageStats {
        self.storage.get_stats().await
    }

    pub async fn clear(&self) -> AuriaResult<()> {
        self.model_store = ModelStore::new();
        self.storage.clear().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_storage() {
        let config = StorageConfig {
            backend: StorageBackend::File,
            file: Some(FileStorageConfig {
                root_path: PathBuf::from("/tmp/auria-test"),
                max_size_bytes: 100 * 1024 * 1024,
            }),
            cache_size: 100,
            ttl: None,
            redis: None,
            s3: None,
            sql: None,
            memory: None,
        };

        let storage = Storage::new(config.clone()).await.unwrap();

        let shard = Shard {
            id: ShardId([1u8; 32]),
            tensor: Tensor {
                data: vec![1.0, 2.0, 3.0],
                dimensions: vec![3],
            },
            metadata: ShardMetadata {
                shard_order: 1,
                dtype: TensorDType::FP32,
                dimensions: vec![3],
                creation_timestamp: 1000,
            },
        };

        // Test put and get
        storage.put_shard(shard.clone()).await.unwrap();
        let loaded = storage.get_shard(shard.id).await.unwrap();
        assert_eq!(loaded.id, shard.id);

        // Test exists
        assert!(storage.exists(shard.id).await);

        // Test delete
        storage.delete_shard(shard.id).await.unwrap();
        assert!(!storage.exists(shard.id).await);
    }

    #[tokio::test]
    async fn test_memory_storage() {
        let config = StorageConfig {
            backend: StorageBackend::Memory,
            memory: Some(MemoryStorageConfig { max_items: 10 }),
            cache_size: 10,
            ttl: None,
            redis: None,
            s3: None,
            sql: None,
            file: None,
        };

        let storage = Storage::new(config.clone()).await.unwrap();

        let shard = Shard {
            id: ShardId([1u8; 32]),
            tensor: Tensor {
                data: vec![1.0, 2.0, 3.0],
                dimensions: vec![3],
            },
            metadata: ShardMetadata {
                shard_order: 1,
                dtype: TensorDType::FP32,
                dimensions: vec![3],
                creation_timestamp: 1000,
            },
        };

        storage.put_shard(shard.clone()).await.unwrap();
        let loaded = storage.get_shard(shard.id).await.unwrap();
        assert_eq!(loaded.id, shard.id);
    }

    #[tokio::test]
    async fn test_storage_stats() {
        let config = StorageConfig {
            backend: StorageBackend::Memory,
            memory: Some(MemoryStorageConfig { max_items: 10 }),
            cache_size: 10,
            ttl: None,
            redis: None,
            s3: None,
            sql: None,
            file: None,
        };

        let storage = Storage::new(config.clone()).await.unwrap();
        let stats = storage.get_stats().await;

        assert_eq!(stats.backend, StorageBackend::Memory);
        assert_eq!(stats.cache_capacity, 10);
    }
}