use serde::Deserialize;

#[derive(Deserialize, Clone, Copy)]
pub struct RetryConfig {
    pub num_tries: Option<usize>,
    pub secs_between_tries: u64,
}

#[derive(Deserialize)]
pub struct DbConfig {
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub host: String,
    pub port: u16,
}
