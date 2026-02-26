use auria_core::{AuriaError, AuriaResult, Shard, ShardId};
use std::num::NonZeroUsize;

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
