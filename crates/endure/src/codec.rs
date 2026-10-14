//! Value encoding
//!
//! Values and map keys are stored as MessagePack with named struct fields. The format is
//! self-describing, so adding a field marked `#[serde(default)]` or removing a field needs no
//! migration, unlike positional formats such as postcard or bincode

use std::cmp::Ordering;

use serde::{Serialize, de::DeserializeOwned};

use crate::error::{Error, Result};

/// Encodes a value for storage in `table`
pub fn encode<T: Serialize + ?Sized>(table: &str, value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(value).map_err(|error| Error::Encode {
        table: table.to_owned(),
        message: error.to_string(),
    })
}

/// Encodes a value and checks that the bytes decode again
///
/// This stops a serde shape that cannot be read back, such as `skip_serializing_if` without
/// `default`, from writing a row that would make the file fail to open
pub fn encode_checked<T: Serialize + DeserializeOwned>(table: &str, value: &T) -> Result<Vec<u8>> {
    let bytes = encode(table, value)?;
    decode::<T>(table, &bytes).map_err(|error| Error::Encode {
        table: table.to_owned(),
        message: format!("the encoded value would not read back: {error}"),
    })?;

    Ok(bytes)
}

/// Encodes a map key and checks that the bytes decode to an equal key
///
/// A key whose encoding loses part of its identity, such as a field marked `#[serde(skip)]`,
/// would share a row with other keys, so a write or delete of one would hit the others
pub fn encode_key<K: Serialize + DeserializeOwned + Ord>(table: &str, key: &K) -> Result<Vec<u8>> {
    let bytes = encode(table, key)?;
    let decoded: K = decode(table, &bytes).map_err(|error| Error::Encode {
        table: table.to_owned(),
        message: format!("the encoded key would not read back: {error}"),
    })?;

    // maps compare keys with `Ord`, so that is the identity the bytes must keep
    if decoded.cmp(key) != Ordering::Equal {
        return Err(Error::Encode {
            table: table.to_owned(),
            message: "the encoded key reads back as a different key".to_owned(),
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
