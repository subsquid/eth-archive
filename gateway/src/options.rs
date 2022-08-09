use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
    #[clap(short, long, value_parser)]
    pub cfg_path: Option<String>,
}

impl Options {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
