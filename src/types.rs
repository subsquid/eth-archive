use web3::types::{Block, Log, Transaction, H256};

pub trait WriteToParquet: Send + Sync + std::fmt::Debug + 'static {}

impl WriteToParquet for Block<H256> {}

impl WriteToParquet for Transaction {}

impl WriteToParquet for Log {}
