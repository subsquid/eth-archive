use std::time::Instant;

pub struct BatchRequest {
    from_block: u64,
    to_block: u64,
    start_time: Instant,
}

pub struct RpcClient {
    url: String,
}