# auria-storage

Shard storage and retrieval for AURIA Runtime Core.

## Storage Hierarchy

- Tier 0: VRAM Cache
- Tier 1: Pinned RAM Cache
- Tier 2: Disk Cache
- Tier 3: Network Storage (IPFS / Arweave)

## Usage

```rust
use auria_storage::Storage;

let mut storage = Storage::new(1000);
storage.store_shard(shard)?;
let loaded = storage.load_shard(shard_id)?;
```
