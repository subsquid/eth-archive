use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    #[clap(short, long)]
    pub block_window_size: usize,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
    pub db_host: String,
    pub db_port: u16,
    #[clap(short, long, value_parser)]
    pub reset_data: bool,
}

impl Options {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
