use std::collections;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed, Xxh3Builder};

pub fn hash_with_seed(input: &[u8], seed: u64) -> u64 {
    xxh3_64_with_seed(input, seed)
}

pub fn hash(input: &[u8]) -> u64 {
    xxh3_64(input)
}

pub type HashMap<K, V> = collections::HashMap<K, V, Xxh3Builder>;
pub type HashSet<V> = collections::HashSet<V>;
