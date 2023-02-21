#![allow(dead_code)]

//! Simple Bloom Filter. copy pasted from https://github.com/solana-labs/solana/blob/master/bloom/src/bloom.rs
use {
    bv::BitVec,
    fnv::FnvHasher,
    rand::{self, Rng},
    serde::{Deserialize, Serialize},
    std::{
        cmp, fmt,
        hash::Hasher,
        marker::PhantomData,
        sync::atomic::{AtomicU64, Ordering},
    },
};

/// Generate a stable hash of `self` for each `hash_index`
/// Best effort can be made for uniqueness of each hash.
pub trait BloomHashIndex {
    fn hash_at_index(&self, hash_index: u64) -> u64;
}

#[derive(Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
pub struct Bloom<T: BloomHashIndex> {
    pub keys: Vec<u64>,
    pub bits: BitVec<u64>,
    num_bits_set: u64,
    _phantom: PhantomData<T>,
}

impl<T: BloomHashIndex> fmt::Debug for Bloom<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Bloom {{ keys.len: {} bits.len: {} num_set: {} bits: ",
            self.keys.len(),
            self.bits.len(),
            self.num_bits_set
        )?;
        const MAX_PRINT_BITS: u64 = 10;
        for i in 0..std::cmp::min(MAX_PRINT_BITS, self.bits.len()) {
            if self.bits.get(i) {
                write!(f, "1")?;
            } else {
                write!(f, "0")?;
            }
        }
        if self.bits.len() > MAX_PRINT_BITS {
            write!(f, "..")?;
        }
        write!(f, " }}")
    }
}

impl<T: BloomHashIndex> Bloom<T> {
    pub fn new(num_bits: usize, keys: Vec<u64>) -> Self {
        let bits = BitVec::new_fill(false, num_bits as u64);
        Bloom {
            keys,
            bits,
            num_bits_set: 0,
            _phantom: PhantomData::default(),
        }
    }
    /// Create filter optimal for num size given the `FALSE_RATE`.
    ///
    /// The keys are randomized for picking data out of a collision resistant hash of size
    /// `keysize` bytes.
    ///
    /// See <https://hur.st/bloomfilter/>.
    pub fn random(num_items: usize, false_rate: f64, max_bits: usize) -> Self {
        let m = Self::num_bits(num_items as f64, false_rate);
        let num_bits = cmp::max(1, cmp::min(m as usize, max_bits));
        let num_keys = Self::num_keys(num_bits as f64, num_items as f64) as usize;
        let keys: Vec<u64> = (0..num_keys).map(|_| rand::thread_rng().gen()).collect();
        Self::new(num_bits, keys)
    }
    fn num_bits(num_items: f64, false_rate: f64) -> f64 {
        let n = num_items;
        let p = false_rate;
        ((n * p.ln()) / (1f64 / 2f64.powf(2f64.ln())).ln()).ceil()
    }
    fn num_keys(num_bits: f64, num_items: f64) -> f64 {
        let n = num_items;
        let m = num_bits;
        // infinity as usize is zero in rust 1.43 but 2^64-1 in rust 1.45; ensure it's zero here
        if n == 0.0 {
            0.0
        } else {
            1f64.max(((m / n) * 2f64.ln()).round())
        }
    }
    fn pos(&self, key: &T, k: u64) -> u64 {
        key.hash_at_index(k).wrapping_rem(self.bits.len())
    }
    pub fn clear(&mut self) {
        self.bits = BitVec::new_fill(false, self.bits.len());
        self.num_bits_set = 0;
    }
    pub fn add(&mut self, key: &T) {
        for k in &self.keys {
            let pos = self.pos(key, *k);
            if !self.bits.get(pos) {
                self.num_bits_set = self.num_bits_set.saturating_add(1);
                self.bits.set(pos, true);
            }
        }
    }
    pub fn contains(&self, key: &T) -> bool {
        for k in &self.keys {
            let pos = self.pos(key, *k);
            if !self.bits.get(pos) {
                return false;
            }
        }
        true
    }
}

fn slice_hash(slice: &[u8], hash_index: u64) -> u64 {
    let mut hasher = FnvHasher::with_key(hash_index);
    hasher.write(slice);
    hasher.finish()
}

impl<T: AsRef<[u8]>> BloomHashIndex for T {
    fn hash_at_index(&self, hash_index: u64) -> u64 {
        slice_hash(self.as_ref(), hash_index)
    }
}

pub struct AtomicBloom<T> {
    num_bits: u64,
    keys: Vec<u64>,
    bits: Vec<AtomicU64>,
    _phantom: PhantomData<T>,
}

impl<T: BloomHashIndex> From<Bloom<T>> for AtomicBloom<T> {
    fn from(bloom: Bloom<T>) -> Self {
        AtomicBloom {
            num_bits: bloom.bits.len(),
            keys: bloom.keys,
            bits: bloom
                .bits
                .into_boxed_slice()
                .iter()
                .map(|&x| AtomicU64::new(x))
                .collect(),
            _phantom: PhantomData::default(),
        }
    }
}

impl<T: BloomHashIndex> AtomicBloom<T> {
    fn pos(&self, key: &T, hash_index: u64) -> (usize, u64) {
        let pos = key.hash_at_index(hash_index).wrapping_rem(self.num_bits);
        // Divide by 64 to figure out which of the
        // AtomicU64 bit chunks we need to modify.
        let index = pos.wrapping_shr(6);
        // (pos & 63) is equivalent to mod 64 so that we can find
        // the index of the bit within the AtomicU64 to modify.
        let mask = 1u64.wrapping_shl(u32::try_from(pos & 63).unwrap());
        (index as usize, mask)
    }

    /// Adds an item to the bloom filter and returns true if the item
    /// was not in the filter before.
    pub fn add(&self, key: &T) -> bool {
        let mut added = false;
        for k in &self.keys {
            let (index, mask) = self.pos(key, *k);
            let prev_val = self.bits[index].fetch_or(mask, Ordering::Relaxed);
            added = added || prev_val & mask == 0u64;
        }
        added
    }

    pub fn contains(&self, key: &T) -> bool {
        self.keys.iter().all(|k| {
            let (index, mask) = self.pos(key, *k);
            let bit = self.bits[index].load(Ordering::Relaxed) & mask;
            bit != 0u64
        })
    }

    pub fn clear_for_tests(&mut self) {
        self.bits.iter().for_each(|bit| {
            bit.store(0u64, Ordering::Relaxed);
        });
    }

    // Only for tests and simulations.
    pub fn mock_clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            bits: self
                .bits
                .iter()
                .map(|v| AtomicU64::new(v.load(Ordering::Relaxed)))
                .collect(),
            ..*self
        }
    }
}

impl<T: BloomHashIndex> From<AtomicBloom<T>> for Bloom<T> {
    fn from(atomic_bloom: AtomicBloom<T>) -> Self {
        let bits: Vec<_> = atomic_bloom
            .bits
            .into_iter()
            .map(AtomicU64::into_inner)
            .collect();
        let num_bits_set = bits.iter().map(|x| x.count_ones() as u64).sum();
        let mut bits: BitVec<u64> = bits.into();
        bits.truncate(atomic_bloom.num_bits);
        Bloom {
            keys: atomic_bloom.keys,
            bits,
            num_bits_set,
            _phantom: PhantomData::default(),
        }
    }
}
