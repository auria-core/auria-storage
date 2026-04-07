// File: sql.rs - This file is part of AURIA
// Copyright (c) 2026 AURIA Developers and Contributors
// Description:
//     SQL database storage backend interface for persistent shard storage.
//     This is a stub implementation - can be extended with actual SQL support.

use auria_core::{AuriaError, AuriaResult, ExpertId, Shard, ShardId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SqlStorageConfig {
    pub database_url: String,
    pub table_name: String,
}

impl SqlStorageConfig {
    pub fn new(database_url: String) -> Self {
        Self {
            database_url,
            table_name: "shards".to_string(),
        }
    }

    pub fn sqlite(path: PathBuf) -> Self {
        let path_str = path.to_string_lossy();
        Self::new(format!("sqlite:{}", path_str))
    }

    pub fn postgres(connection_string: String) -> Self {
        Self::new(connection_string)
    }

    pub fn with_table_name(mut self, name: String) -> Self {
        self.table_name = name;
        self
    }
}

#[derive(Clone)]
pub struct SqlStorage {
    config: SqlStorageConfig,
    cache: Arc<RwLock<lru::LruCache<ShardId, Shard>>>,
    max_cache_size: usize,
    store: Arc<RwLock<HashMap<ShardId, Shard>>>,
}

impl SqlStorage {
    pub fn new(config: SqlStorageConfig, cache_size: usize) -> Self {
        let cache = lru::LruCache::new(
            NonZeroUsize::new(cache_size.max(1)).unwrap(),
        );
        Self {
            config,
            cache: Arc::new(RwLock::new(cache)),
            max_cache_size: cache_size,
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn init(&self) -> AuriaResult<()> {
        Ok(())
    }

    pub async fn get_shard(&self, shard_id: ShardId) -> AuriaResult<Shard> {
        {
            let mut cache = self.cache.write().await;
            if let Some(shard) = cache.get(&shard_id) {
                return Ok(shard.clone());
            }
        }

        {
            let store = self.store.read().await;
            if let Some(shard) = store.get(&shard_id) {
                let shard_clone = shard.clone();
                
                let mut cache = self.cache.write().await;
                cache.put(shard_id, shard_clone.clone());
                
                return Ok(shard_clone);
            }
        }

        Err(AuriaError::ShardNotFound(shard_id))
    }

    pub async fn put_shard(&self, shard: Shard) -> AuriaResult<()> {
        {
            let mut store = self.store.write().await;
            store.insert(shard.shard_id, shard.clone());
        }

        {
            let mut cache = self.cache.write().await;
            if cache.len() >= self.max_cache_size {
                cache.pop_lru();
            }
            cache.put(shard.shard_id, shard);
        }

        Ok(())
    }

    pub async fn delete_shard(&self, shard_id: ShardId) -> AuriaResult<()> {
        {
            let mut store = self.store.write().await;
            store.remove(&shard_id);
        }

        {
            let mut cache = self.cache.write().await;
            cache.pop(&shard_id);
        }

        Ok(())
    }

    pub async fn exists(&self, shard_id: ShardId) -> bool {
        if self.cache.write().await.contains(&shard_id) {
            return true;
        }

        self.store.read().await.contains_key(&shard_id)
    }

    pub async fn list_shards(&self, _model_id: Option<ShardId>, limit: Option<usize>) -> AuriaResult<Vec<ShardId>> {
        let store = self.store.read().await;
        
        let mut shard_ids: Vec<ShardId> = store.keys().cloned().collect();
        
        if let Some(l) = limit {
            shard_ids.truncate(l);
        }
        
        Ok(shard_ids)
    }

    pub async fn get_shards_by_expert(&self, expert_id: ExpertId) -> AuriaResult<Vec<Shard>> {
        let store = self.store.read().await;
        
        let shards: Vec<Shard> = store.values()
            .filter(|s| s.expert_id == expert_id)
            .cloned()
            .collect();
        
        Ok(shards)
    }

    pub async fn count_shards(&self) -> AuriaResult<u64> {
        Ok(self.store.read().await.len() as u64)
    }

    pub async fn get_storage_size(&self) -> AuriaResult<u64> {
        let store = self.store.read().await;
        
        let mut total_size = 0u64;
        for shard in store.values() {
            total_size += serde_json::to_vec(&shard)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
        }
        
        Ok(total_size)
    }

    pub async fn clear(&self) -> AuriaResult<()> {
        {
            let mut store = self.store.write().await;
            store.clear();
        }
        {
            let mut cache = self.cache.write().await;
            cache.clear();
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlStorageStats {
    pub cache_size: usize,
    pub cache_capacity: usize,
    pub total_shards: usize,
    pub table_name: String,
    pub database_url: String,
}

impl SqlStorage {
    pub async fn get_stats(&self) -> SqlStorageStats {
        let cache = self.cache.read().await;
        let store = self.store.read().await;
        
        SqlStorageStats {
            cache_size: cache.len(),
            cache_capacity: self.max_cache_size,
            total_shards: store.len(),
            table_name: self.config.table_name.clone(),
            database_url: self.config.database_url.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_config() {
        let config = SqlStorageConfig::new("sqlite:test.db".to_string());
        
        assert_eq!(config.database_url, "sqlite:test.db");
    }

    #[test]
    fn test_sqlite_config() {
        let config = SqlStorageConfig::sqlite(PathBuf::from("/data/auria.db"));
        
        assert!(config.database_url.contains("auria.db"));
    }

    #[test]
    fn test_postgres_config() {
        let config = SqlStorageConfig::postgres("postgres://localhost/auria".to_string());
        
        assert!(config.database_url.contains("postgres"));
    }

    #[test]
    fn test_sql_storage_creation() {
        let config = SqlStorageConfig::new("memory".to_string());
        let storage = SqlStorage::new(config, 100);
        
        assert_eq!(storage.max_cache_size, 100);
    }

    #[test]
    fn test_sql_storage_operations() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        
        let config = SqlStorageConfig::new("memory".to_string());
        let storage = SqlStorage::new(config, 100);
        
        let shard = Shard {
            shard_id: ShardId([1u8; 32]),
            expert_id: ExpertId([2u8; 32]),
            tensor: auria_core::Tensor {
                data: vec![1, 2, 3, 4],
                shape: vec![2, 2],
                dtype: auria_core::TensorDType::FP16,
            },
            metadata: auria_core::ShardMetadata {
                owner: auria_core::PublicKey([0u8; 32]),
                license_hash: None,
                created_at: 1000,
                version: 1,
            },
        };
        
        rt.block_on(async {
            storage.put_shard(shard.clone()).await.unwrap();
            assert!(storage.exists(shard.shard_id).await);
            
            let loaded = storage.get_shard(shard.shard_id).await.unwrap();
            assert_eq!(loaded.shard_id, shard.shard_id);
            
            storage.delete_shard(shard.shard_id).await.unwrap();
            assert!(!storage.exists(shard.shard_id).await);
        });
    }
}
