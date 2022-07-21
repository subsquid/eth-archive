use serde::Deserialize;

#[derive(Deserialize, Clone, Copy)]
pub struct RetryConfig {
    pub num_tries: Option<usize>,
    pub secs_between_tries: u64,
}
