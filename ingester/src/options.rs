use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    /// An ethereum node rpc url
    #[clap(short, long)]
    pub eth_rpc_url: url::Url,

    /// Number of concurrent requests to an rpc node
    #[clap(long, default_value_t = 1)]
    pub http_req_concurrency: usize,

    /// Count of blocks to be downloaded by a batch concurrent request
    #[clap(long, default_value_t = 10)]
    pub block_batch_size: usize,

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

    /// Block window size
    #[clap(long)]
    pub block_window_size: usize,

    /// Delete indexed data from a database
    #[clap(short, long, value_parser)]
    pub reset_data: bool,
}

impl Options {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
