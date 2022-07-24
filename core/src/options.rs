use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    #[clap(short, long, value_parser)]
    pub reset_db: bool,
    #[clap(short, long, value_parser, default_value = "EthIngester.toml")]
    pub cfg_path: String,
}

impl Options {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
