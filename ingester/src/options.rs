use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    #[clap(short, long, value_parser)]
    pub(crate) reset_db: bool,
}
