// File: lib.rs - This file is part of AURIA
// Copyright (c) 2026 AURIA Developers and Contributors
// Description:
//     Shard storage and retrieval for AURIA Runtime Core.
//     Implements a hierarchical storage system with LRU caching for efficient
//     shard loading from VRAM, RAM, disk, and network storage tiers.
//
pub mod s3;
pub mod redis;
pub mod sql;

use auria_core::{AuriaError, AuriaResult, Shard, ShardId};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock as AsyncRwLock;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    Vram,
    Ram,
    Disk,
    Network,
}

pub struct StorageTierConfig {
    pub tier: StorageTier,
    pub max_size_bytes: u64,
    pub path: Option<PathBuf>,
}

pub struct MultiTierStorage {
    vram_cache: Arc<AsyncRwLock<lru::LruCache<ShardId, Shard>>>,
    ram_cache: Arc<AsyncRwLock<lru::LruCache<ShardId, Shard>>>,
    disk_cache: Arc<AsyncRwLock<HashMap<ShardId, PathBuf>>>,
    disk_root: PathBuf,
}

impl MultiTierStorage {
    pub fn new(disk_root: PathBuf, vram_capacity: usize, ram_capacity: usize) -> Self {
        Self {
            vram_cache: Arc::new(AsyncRwLock::new(
                lru::LruCache::new(NonZeroUsize::new(vram_capacity).unwrap_or(NonZeroUsize::new(1).unwrap()))
            )),
            ram_cache: Arc::new(AsyncRwLock::new(
                lru::LruCache::new(NonZeroUsize::new(ram_capacity).unwrap_or(NonZeroUsize::new(1).unwrap()))
            )),
            disk_cache: Arc::new(AsyncRwLock::new(HashMap::new())),
            disk_root,
        }
    }

    pub async fn load_shard(&self, shard_id: ShardId) -> AuriaResult<Shard> {
        {
            let mut cache = self.vram_cache.write().await;
            if let Some(shard) = cache.get(&shard_id).cloned() {
                return Ok(shard);
            }
        }

        {
            let mut cache = self.ram_cache.write().await;
            if let Some(shard) = cache.get(&shard_id).cloned() {
                let shard_clone = shard.clone();
                self.vram_cache.write().await.put(shard_id, shard_clone);
                return Ok(shard);
            }
        }

        {
            let cache = self.disk_cache.read().await;
            if let Some(path) = cache.get(&shard_id) {
                let path = path.clone();
                drop(cache);
                let shard = self.load_from_disk(&path).await?;
                self.vram_cache.write().await.put(shard.shard_id, shard.clone());
                return Ok(shard);
            }
        }

        Err(AuriaError::ShardNotFound(shard_id))
    }

    pub async fn store_shard(&self, shard: Shard) -> AuriaResult<()> {
        self.vram_cache.write().await.put(shard.shard_id, shard.clone());
        self.ram_cache.write().await.put(shard.shard_id, shard.clone());
        self.save_to_disk(&shard).await?;
        Ok(())
    }

    pub async fn shard_exists(&self, shard_id: ShardId) -> bool {
        if self.vram_cache.read().await.contains(&shard_id) {
            return true;
        }
        if self.ram_cache.read().await.contains(&shard_id) {
            return true;
        }
        if self.disk_cache.read().await.contains_key(&shard_id) {
            return true;
        }
        false
    }

    pub async fn move_to_vram(&self, shard_id: ShardId) -> AuriaResult<()> {
        if let Some(shard) = self.ram_cache.write().await.pop(&shard_id) {
            self.vram_cache.write().await.put(shard_id, shard);
        }
        Ok(())
    }

    pub async fn evict_from_vram(&self, shard_id: ShardId) -> AuriaResult<()> {
        if let Some(shard) = self.vram_cache.write().await.pop(&shard_id) {
            self.ram_cache.write().await.put(shard_id, shard);
        }
        Ok(())
    }

    async fn load_from_disk(&self, path: &PathBuf) -> AuriaResult<Shard> {
        let data = tokio::fs::read(path).await
            .map_err(|e| AuriaError::StorageError(format!("Failed to read shard from disk: {}", e)))?;
        let shard: Shard = serde_json::from_slice(&data)
            .map_err(|e| AuriaError::SerializationError(format!("Failed to deserialize shard: {}", e)))?;
        Ok(shard)
    }

    async fn save_to_disk(&self, shard: &Shard) -> AuriaResult<()> {
        let path = self.disk_root.join(hex::encode(shard.shard_id.0));
        let data = serde_json::to_vec(shard)
            .map_err(|e| AuriaError::SerializationError(format!("Failed to serialize shard: {}", e)))?;
        tokio::fs::create_dir_all(&self.disk_root).await.ok();
        tokio::fs::write(&path, data).await
            .map_err(|e| AuriaError::StorageError(format!("Failed to write shard to disk: {}", e)))?;
        self.disk_cache.write().await.insert(shard.shard_id, path);
        Ok(())
    }

    pub async fn get_storage_stats(&self) -> StorageStats {
        let vram_count = self.vram_cache.read().await.len();
        let ram_count = self.ram_cache.read().await.len();
        let disk_count = self.disk_cache.read().await.len() as u64;
        StorageStats {
            vram_count,
            ram_count,
            disk_count,
        }
    }
}

pub struct StorageStats {
    pub vram_count: usize,
    pub ram_count: usize,
    pub disk_count: u64,
}

pub struct Storage {
    cache: lru::LruCache<ShardId, Shard>,
    max_items: usize,
}

impl Storage {
    pub fn new(max_items: usize) -> Self {
        Self {
            cache: lru::LruCache::new(NonZeroUsize::new(max_items).unwrap()),
            max_items,
        }
    }

    pub fn load_shard(&mut self, shard_id: ShardId) -> AuriaResult<Shard> {
        if let Some(shard) = self.cache.get(&shard_id) {
            return Ok(shard.clone());
        }
        Err(AuriaError::ShardNotFound(shard_id))
    }

    pub fn store_shard(&mut self, shard: Shard) -> AuriaResult<()> {
        self.cache.put(shard.shard_id, shard);
        Ok(())
    }

    pub fn shard_exists(&self, shard_id: &ShardId) -> bool {
        self.cache.contains(shard_id)
    }
}
