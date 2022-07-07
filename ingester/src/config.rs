use scylla::transport::Compression as ScyllaCompression;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub db: DbConfig,
}

#[derive(Deserialize)]
pub struct DbConfig {
    pub auth: Option<Auth>,
    pub compression: Option<Compression>,
    pub known_nodes: Vec<String>,
}

#[derive(Deserialize)]
pub struct Auth {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Clone, Copy)]
pub enum Compression {
    Lz4,
    Snappy,
}

impl From<Compression> for ScyllaCompression {
    fn from(compression: Compression) -> ScyllaCompression {
        match compression {
            Compression::Lz4 => ScyllaCompression::Lz4,
            Compression::Snappy => ScyllaCompression::Snappy,
        }
    }
}
