mod config;
mod db;
mod error;
mod ingester;

pub use error::{Error, Result};
pub use ingester::Ingester;
