//! Value encoding
//!
//! Values and map keys are stored as MessagePack with named struct fields. The format is
//! self-describing, so adding a field marked `#[serde(default)]` or removing a field needs no
//! migration, unlike positional formats such as postcard or bincode

use serde::{Serialize, de::DeserializeOwned};

use crate::error::{Error, Result};

/// Encodes a value for storage in `table`
pub fn encode<T: Serialize + ?Sized>(table: &str, value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(value).map_err(|error| Error::Encode {
        table: table.to_owned(),
        message: error.to_string(),
    })
}

/// Encodes a value and checks that it reads back to the same bytes
///
/// This stops a serde shape that cannot round-trip, such as `skip_serializing_if` without
/// `default`, from writing a row that would make the file fail to open
pub fn encode_checked<T: Serialize + DeserializeOwned>(table: &str, value: &T) -> Result<Vec<u8>> {
    let bytes = encode(table, value)?;
    let decoded: T = decode(table, &bytes).map_err(|error| Error::Encode {
        table: table.to_owned(),
        message: format!("the encoded value would not read back: {error}"),
    })?;

    if encode(table, &decoded)? != bytes {
        return Err(Error::Encode {
            table: table.to_owned(),
            message: "the value changes when read back".to_owned(),
        });
    }

    Ok(bytes)
}

/// Decodes a value stored in `table`
pub fn decode<T: DeserializeOwned>(table: &str, bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(|error| Error::Decode {
        table: table.to_owned(),
        message: error.to_string(),
    })
}
