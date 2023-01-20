#![deny(warnings)]

mod config;
mod error;
mod ingester;
mod schema;
mod server;

pub use config::Config;
pub use error::{Error, Result};
pub use ingester::Ingester;
