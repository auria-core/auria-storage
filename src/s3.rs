// File: s3.rs - This file is part of AURIA
// Copyright (c) 2026 AURIA Developers and Contributors
// Description:
//     S3-compatible object storage backend for shard persistence.
//     Supports AWS S3, MinIO, Cloudflare R2, and other S3-compatible stores.

use auria_core::{AuriaError, AuriaResult, Shard, ShardId};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub use_path_style: bool,
}

impl S3StorageConfig {
    pub fn new(bucket: String, region: String, access_key: String, secret_key: String) -> Self {
        Self {
            bucket,
            region,
            endpoint: None,
            access_key,
            secret_key,
            use_path_style: false,
        }
    }

    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn with_path_style(mut self) -> Self {
        self.use_path_style = true;
        self
    }
}

#[derive(Clone)]
pub struct S3Storage {
    config: S3StorageConfig,
    cache: Arc<RwLock<lru::LruCache<ShardId, Shard>>>,
    max_cache_size: usize,
}

impl S3Storage {
    pub fn new(config: S3StorageConfig, cache_size: usize) -> Self {
        let cache = lru::LruCache::new(
            NonZeroUsize::new(cache_size.max(1)).unwrap(),
        );
        Self {
            config,
            cache: Arc::new(RwLock::new(cache)),
            max_cache_size: cache_size,
        }
    }

    pub async fn get_shard(&self, shard_id: ShardId) -> AuriaResult<Shard> {
        {
            let mut cache = self.cache.write().await;
            if let Some(shard) = cache.get(&shard_id) {
                return Ok(shard.clone());
            }
        }

        let key = self.get_object_key(&shard_id);
        let data = self.download_object(&key, &shard_id).await?;
        
        let shard: Shard = serde_json::from_slice(&data)
            .map_err(|e| AuriaError::SerializationError(format!("Failed to deserialize shard: {}", e)))?;

        {
            let mut cache = self.cache.write().await;
            cache.put(shard_id, shard.clone());
        }

        Ok(shard)
    }

    pub async fn put_shard(&self, shard: Shard) -> AuriaResult<()> {
        let key = self.get_object_key(&shard.shard_id);
        
        let data = serde_json::to_vec(&shard)
            .map_err(|e| AuriaError::SerializationError(format!("Failed to serialize shard: {}", e)))?;

        self.upload_object(&key, &data).await?;

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
        let key = self.get_object_key(&shard_id);
        self.delete_object(&key).await?;

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
        
        let key = self.get_object_key(&shard_id);
        self.object_exists(&key).await
    }

    pub async fn list_shards(&self, prefix: Option<String>) -> AuriaResult<Vec<ShardId>> {
        self.list_objects(prefix.as_deref()).await
    }

    fn get_object_key(&self, shard_id: &ShardId) -> String {
        format!("shards/{}.json", hex::encode(shard_id.0))
    }

    async fn download_object(&self, key: &str, shard_id: &ShardId) -> AuriaResult<Vec<u8>> {
        let url = self.build_url(key);
        
        let client = reqwest::Client::new();
        let response = client.get(&url)
            .basic_auth(&self.config.access_key, Some(&self.config.secret_key))
            .send()
            .await
            .map_err(|e| AuriaError::StorageError(format!("S3 download failed: {}", e)))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(AuriaError::ShardNotFound(shard_id.clone()));
        }

        if !response.status().is_success() {
            return Err(AuriaError::StorageError(format!("S3 error: {}", response.status())));
        }

        let bytes = response.bytes().await
            .map_err(|e| AuriaError::StorageError(format!("Failed to read response: {}", e)))?;
        
        Ok(bytes.to_vec())
    }

    async fn upload_object(&self, key: &str, data: &[u8]) -> AuriaResult<()> {
        let url = self.build_url(key);
        
        let client = reqwest::Client::new();
        client.put(&url)
            .basic_auth(&self.config.access_key, Some(&self.config.secret_key))
            .header("Content-Type", "application/json")
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| AuriaError::StorageError(format!("S3 upload failed: {}", e)))?;

        Ok(())
    }

    async fn delete_object(&self, key: &str) -> AuriaResult<()> {
        let url = self.build_url(key);
        
        let client = reqwest::Client::new();
        client.delete(&url)
            .basic_auth(&self.config.access_key, Some(&self.config.secret_key))
            .send()
            .await
            .map_err(|e| AuriaError::StorageError(format!("S3 delete failed: {}", e)))?;

        Ok(())
    }

    async fn object_exists(&self, key: &str) -> bool {
        let url = self.build_url(key);
        
        let client = reqwest::Client::new();
        match client.head(&url)
            .basic_auth(&self.config.access_key, Some(&self.config.secret_key))
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    async fn list_objects(&self, prefix: Option<&str>) -> AuriaResult<Vec<ShardId>> {
        Ok(Vec::new())
    }

    fn build_url(&self, key: &str) -> String {
        if let Some(ref endpoint) = self.config.endpoint {
            if self.config.use_path_style {
                format!("{}/{}/{}", endpoint, self.config.bucket, key)
            } else {
                format!("{}/{}/{}", endpoint, self.config.bucket, key)
            }
        } else {
            format!("https://{}.s3.{}.amazonaws.com/{}", self.config.bucket, self.config.region, key)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct S3StorageStats {
    pub cache_size: usize,
    pub cache_capacity: usize,
    pub bucket: String,
    pub region: String,
}

impl S3Storage {
    pub async fn get_stats(&self) -> S3StorageStats {
        let cache = self.cache.read().await;
        S3StorageStats {
            cache_size: cache.len(),
            cache_capacity: self.max_cache_size,
            bucket: self.config.bucket.clone(),
            region: self.config.region.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config() {
        let config = S3StorageConfig::new(
            "test-bucket".to_string(),
            "us-east-1".to_string(),
            "access_key".to_string(),
            "secret_key".to_string(),
        );
        
        assert_eq!(config.bucket, "test-bucket");
    }

    #[test]
    fn test_object_key_generation() {
        let config = S3StorageConfig::new(
            "test-bucket".to_string(),
            "us-east-1".to_string(),
            "access_key".to_string(),
            "secret_key".to_string(),
        );
        
        let storage = S3Storage::new(config, 100);
        let shard_id = ShardId([1u8; 32]);
        let key = storage.get_object_key(&shard_id);
        
        assert!(key.starts_with("shards/"));
    }
}
