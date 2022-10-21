use clap::Parser;
use std::net::Ipv4Addr;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    /// A path indexed parquet files
    #[clap(short, long)]
    pub data_path: PathBuf,

    /// Query chunk size
    #[clap(long)]
    pub query_chunk_size: u32,

    /// Time limit to execute query
    #[clap(long)]
    pub query_time_limit_ms: u64,

    /// Ip to be used for running server
    #[clap(long, default_value_t = Ipv4Addr::new(127, 0, 0, 1))]
    pub ip: Ipv4Addr,

    /// Port to be used for running server
    #[clap(long, short, default_value_t = 8080)]
    pub port: u16,

    /// Database user
    #[clap(long)]
    pub db_user: String,

    /// Database user's password
    #[clap(long)]
    pub db_password: String,

    /// Database name
    #[clap(long)]
    pub db_name: String,

    /// Database host
    #[clap(long)]
    pub db_host: String,

    /// Database port
    #[clap(long)]
    pub db_port: u16,
}

impl Options {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
