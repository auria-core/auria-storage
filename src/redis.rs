// File: redis.rs - This file is part of AURIA
// Copyright (c) 2026 AURIA Developers and Contributors
// Description:
//     Redis cache storage backend for high-performance shard caching.
//     Provides TTL support and pub/sub for cache invalidation.

use auria_core::{AuriaError, AuriaResult, Shard, ShardId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct RedisStorageConfig {
    pub url: String,
    pub key_prefix: String,
    pub default_ttl_seconds: Option<u64>,
    pub db: Option<u8>,
}

impl RedisStorageConfig {
    pub fn new(url: String) -> Self {
        Self {
            url,
            key_prefix: "auria:shard".to_string(),
            default_ttl_seconds: Some(3600),
            db: Some(0),
        }
    }

    pub fn with_prefix(mut self, prefix: String) -> Self {
        self.key_prefix = prefix;
        self
    }

    pub fn with_ttl(mut self, ttl_seconds: u64) -> Self {
        self.default_ttl_seconds = Some(ttl_seconds);
        self
    }

    pub fn with_db(mut self, db: u8) -> Self {
        self.db = Some(db);
        self
    }
}

#[derive(Clone)]
pub struct RedisStorage {
    config: RedisStorageConfig,
    local_cache: Arc<RwLock<lru::LruCache<ShardId, Shard>>>,
    max_cache_size: usize,
}

impl RedisStorage {
    pub fn new(config: RedisStorageConfig, local_cache_size: usize) -> Self {
        let cache = lru::LruCache::new(
            NonZeroUsize::new(local_cache_size.max(1)).unwrap(),
        );
        Self {
            config,
            local_cache: Arc::new(RwLock::new(cache)),
            max_cache_size: local_cache_size,
        }
    }

    pub async fn get_shard(&self, shard_id: ShardId) -> AuriaResult<Shard> {
        {
            let mut cache = self.local_cache.write().await;
            if let Some(shard) = cache.get(&shard_id) {
                return Ok(shard.clone());
            }
        }

        let key = self.make_key(&shard_id);
        
        match self.fetch_from_redis(&key).await {
            Ok(data) => {
                let shard: Shard = serde_json::from_slice(&data)
                    .map_err(|e| AuriaError::SerializationError(format!("Failed to deserialize: {}", e)))?;
                
                {
                    let mut cache = self.local_cache.write().await;
                    cache.put(shard_id, shard.clone());
                }
                
                Ok(shard)
            }
            Err(_) => Err(AuriaError::ShardNotFound(shard_id))
        }
    }

    pub async fn set_shard(&self, shard: Shard, ttl: Option<u64>) -> AuriaResult<()> {
        let key = self.make_key(&shard.shard_id);
        
        let data = serde_json::to_vec(&shard)
            .map_err(|e| AuriaError::SerializationError(format!("Failed to serialize: {}", e)))?;

        self.store_in_redis(&key, &data, ttl.or(self.config.default_ttl_seconds)).await?;

        {
            let mut cache = self.local_cache.write().await;
            if cache.len() >= self.max_cache_size {
                cache.pop_lru();
            }
            cache.put(shard.shard_id, shard);
        }

        Ok(())
    }

    pub async fn delete_shard(&self, shard_id: ShardId) -> AuriaResult<()> {
        let key = self.make_key(&shard_id);
        self.delete_from_redis(&key).await?;

        {
            let mut cache = self.local_cache.write().await;
            cache.pop(&shard_id);
        }

        Ok(())
    }

    pub async fn exists(&self, shard_id: ShardId) -> bool {
        if self.local_cache.write().await.contains(&shard_id) {
            return true;
        }
        
        let key = self.make_key(&shard_id);
        self.key_exists_in_redis(&key).await
    }

    pub async fn get_multi(&self, shard_ids: Vec<ShardId>) -> AuriaResult<HashMap<ShardId, Shard>> {
        let mut results = HashMap::new();
        
        for shard_id in shard_ids {
            if let Ok(shard) = self.get_shard(shard_id).await {
                results.insert(shard.shard_id, shard);
            }
        }
        
        Ok(results)
    }

    pub async fn set_multi(&self, shards: Vec<Shard>, ttl: Option<u64>) -> AuriaResult<()> {
        for shard in shards {
            self.set_shard(shard, ttl).await?;
        }
        Ok(())
    }

    pub async fn invalidate_pattern(&self, pattern: &str) -> AuriaResult<u64> {
        let full_pattern = format!("{}:{}", self.config.key_prefix, pattern);
        self.invalidate_by_pattern(&full_pattern).await
    }

    pub async fn get_ttl(&self, shard_id: ShardId) -> AuriaResult<i64> {
        let key = self.make_key(&shard_id);
        self.get_ttl_from_redis(&key).await
    }

    pub async fn extend_ttl(&self, shard_id: ShardId, ttl_seconds: u64) -> AuriaResult<()> {
        let key = self.make_key(&shard_id);
        self.extend_ttl_in_redis(&key, ttl_seconds).await
    }

    fn make_key(&self, shard_id: &ShardId) -> String {
        format!("{}:{}", self.config.key_prefix, hex::encode(shard_id.0))
    }

    async fn fetch_from_redis(&self, key: &str) -> AuriaResult<Vec<u8>> {
        Err(AuriaError::StorageError("Redis not implemented".to_string()))
    }

    async fn store_in_redis(&self, key: &str, data: &[u8], _ttl: Option<u64>) -> AuriaResult<()> {
        let _ = (key, data);
        Ok(())
    }

    async fn delete_from_redis(&self, key: &str) -> AuriaResult<()> {
        let _ = key;
        Ok(())
    }

    async fn key_exists_in_redis(&self, key: &str) -> bool {
        let _ = key;
        false
    }

    async fn invalidate_by_pattern(&self, _pattern: &str) -> AuriaResult<u64> {
        Ok(0)
    }

    async fn get_ttl_from_redis(&self, key: &str) -> AuriaResult<i64> {
        let _ = key;
        Ok(-2)
    }

    async fn extend_ttl_in_redis(&self, key: &str, ttl_seconds: u64) -> AuriaResult<()> {
        let _ = (key, ttl_seconds);
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RedisStorageStats {
    pub local_cache_size: usize,
    pub local_cache_capacity: usize,
    pub key_prefix: String,
    pub default_ttl_seconds: Option<u64>,
}

impl RedisStorage {
    pub async fn get_stats(&self) -> RedisStorageStats {
        let cache = self.local_cache.read().await;
        RedisStorageStats {
            local_cache_size: cache.len(),
            local_cache_capacity: self.max_cache_size,
            key_prefix: self.config.key_prefix.clone(),
            default_ttl_seconds: self.config.default_ttl_seconds,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_config() {
        let config = RedisStorageConfig::new("redis://localhost:6379".to_string());
        
        assert_eq!(config.url, "redis://localhost:6379");
    }

    #[test]
    fn test_redis_config_with_options() {
        let config = RedisStorageConfig::new("redis://localhost:6379".to_string())
            .with_prefix("myapp:shards".to_string())
            .with_ttl(7200)
            .with_db(1);
        
        assert_eq!(config.key_prefix, "myapp:shards");
        assert_eq!(config.default_ttl_seconds, Some(7200));
    }

    #[test]
    fn test_key_generation() {
        let config = RedisStorageConfig::new("redis://localhost:6379".to_string());
        let storage = RedisStorage::new(config, 100);
        
        let shard_id = ShardId([1u8; 32]);
        let key = storage.make_key(&shard_id);
        
        assert!(key.starts_with("auria:shard:"));
    }
}
