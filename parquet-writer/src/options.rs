use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    #[clap(short, long, value_parser, default_value = "EthParquetWriter.toml")]
    pub(crate) cfg_path: String,
}
