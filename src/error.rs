use std::result::Result as StdResult;

pub enum Error {
    Hex(hex::FromHexError),
    PushRow(arrow2::error::ArrowError),
}

pub type Result<T> = StdResult<T, Error>;
