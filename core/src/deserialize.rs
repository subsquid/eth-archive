use prefix_hex::ToHexPrefixed;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryInto;
use std::fmt;

#[derive(Debug, Clone, derive_more::Deref, derive_more::From, PartialEq, Eq)]
pub struct Bytes32(pub Box<[u8; 32]>);

#[derive(Debug, Clone, derive_more::Deref, derive_more::From, PartialEq, Eq)]
pub struct Address(pub Box<[u8; 20]>);

#[derive(Debug, Clone, derive_more::Deref, derive_more::From, PartialEq, Eq)]
pub struct Sighash(pub Box<[u8; 4]>);

#[derive(Debug, Clone, Copy, derive_more::Deref, derive_more::From, PartialEq, Eq)]
pub struct Nonce(pub u64);

#[derive(Debug, Clone, derive_more::Deref, derive_more::From, PartialEq, Eq)]
pub struct BloomFilterBytes(pub Box<[u8; 256]>);

#[derive(Debug, Clone, Copy, derive_more::Deref, derive_more::From, PartialEq, Eq)]
pub struct BigInt(pub i64);

#[derive(Debug, Clone, Copy, derive_more::Deref, derive_more::From, PartialEq, Eq)]
pub struct Index(pub u32);

#[derive(Debug, Clone, derive_more::Deref, derive_more::From, PartialEq, Eq)]
pub struct Bytes(pub Vec<u8>);

impl Bytes32 {
    pub fn new(bytes: &[u8]) -> Self {
        match bytes.try_into() {
            Ok(b) => Self(Box::new(b)),
            Err(_) => {
                dbg!(bytes);
                panic!("anan");
            }
        }
    }
}

impl ToHexPrefixed for &Bytes32 {
    fn to_hex_prefixed(self) -> String {
        ToHexPrefixed::to_hex_prefixed(*self.0)
    }
}

impl Address {
    pub fn new(bytes: &[u8]) -> Self {
        Self(Box::new(bytes.try_into().unwrap()))
    }
}

impl AsRef<[u8]> for Address {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl ToHexPrefixed for &Address {
    fn to_hex_prefixed(self) -> String {
        ToHexPrefixed::to_hex_prefixed(*self.0)
    }
}

impl Sighash {
    pub fn new(bytes: &[u8]) -> Self {
        Self(Box::new(bytes.try_into().unwrap()))
    }
}

impl ToHexPrefixed for &Sighash {
    fn to_hex_prefixed(self) -> String {
        ToHexPrefixed::to_hex_prefixed(*self.0)
    }
}

impl Nonce {
    pub fn new(bytes: &[u8]) -> Self {
        Self(u64::from_be_bytes(bytes.try_into().unwrap()))
    }
}

impl BloomFilterBytes {
    pub fn new(bytes: &[u8]) -> Self {
        Self(Box::new(bytes.try_into().unwrap()))
    }
}

impl Index {
    pub fn new(val: i64) -> Self {
        Self(val.try_into().unwrap())
    }
}

impl Bytes {
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.to_owned())
    }
}

struct Bytes32Visitor;

impl<'de> Visitor<'de> for Bytes32Visitor {
    type Value = Bytes32;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for 32 byte value")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let buf: [u8; 32] = prefix_hex::decode(value).map_err(|e| E::custom(e.to_string()))?;

        Ok(Box::new(buf).into())
    }
}

impl<'de> Deserialize<'de> for Bytes32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(Bytes32Visitor)
    }
}

impl Serialize for Bytes32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex = prefix_hex::encode(self);

        serializer.serialize_str(&hex)
    }
}

struct AddressVisitor;

impl<'de> Visitor<'de> for AddressVisitor {
    type Value = Address;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for 20 byte address")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let buf: [u8; 20] = prefix_hex::decode(value).map_err(|e| E::custom(e.to_string()))?;

        Ok(Box::new(buf).into())
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(AddressVisitor)
    }
}

impl Serialize for Address {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex = prefix_hex::encode(self);

        serializer.serialize_str(&hex)
    }
}

struct SighashVisitor;

impl<'de> Visitor<'de> for SighashVisitor {
    type Value = Sighash;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for 4 byte sighash")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let buf: [u8; 4] = prefix_hex::decode(value).map_err(|e| E::custom(e.to_string()))?;

        Ok(Box::new(buf).into())
    }
}

impl<'de> Deserialize<'de> for Sighash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SighashVisitor)
    }
}

impl Serialize for Sighash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex = prefix_hex::encode(self);

        serializer.serialize_str(&hex)
    }
}

struct NonceVisitor;

impl<'de> Visitor<'de> for NonceVisitor {
    type Value = Nonce;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for 8 byte nonce")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let value = match value.strip_prefix("0x") {
            Some(value) => u64::from_str_radix(value, 16).map_err(|e| E::custom(e.to_string()))?,
            None => value.parse::<u64>().map_err(|e| E::custom(e.to_string()))?,
        };

        Ok(Nonce(value))
    }
}

impl<'de> Deserialize<'de> for Nonce {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(NonceVisitor)
    }
}

impl Serialize for Nonce {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

struct BloomFilterBytesVisitor;

impl<'de> Visitor<'de> for BloomFilterBytesVisitor {
    type Value = BloomFilterBytes;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for 256 byte bloom filter")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let buf: [u8; 256] = prefix_hex::decode(value).map_err(|e| E::custom(e.to_string()))?;

        Ok(Box::new(buf).into())
    }
}

impl<'de> Deserialize<'de> for BloomFilterBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BloomFilterBytesVisitor)
    }
}

impl Serialize for BloomFilterBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex = prefix_hex::encode(*self.0);

        serializer.serialize_str(&hex)
    }
}

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Bytes;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("arbitrary length hex string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let buf: Vec<u8> = if value.len() % 2 != 0 {
            let value = format!("0x0{}", &value[2..]);
            prefix_hex::decode(&value).map_err(|e| E::custom(e.to_string()))?
        } else {
            prefix_hex::decode(value).map_err(|e| E::custom(e.to_string()))?
        };

        Ok(buf.into())
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BytesVisitor)
    }
}

impl Serialize for Bytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex = prefix_hex::encode(&*self.0);

        serializer.serialize_str(&hex)
    }
}

struct BigIntVisitor;

impl<'de> Visitor<'de> for BigIntVisitor {
    type Value = BigInt;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for 8 byte value")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let value = match value.strip_prefix("0x") {
            Some(value) => i64::from_str_radix(value, 16).map_err(|e| E::custom(e.to_string()))?,
            None => value.parse::<i64>().map_err(|e| E::custom(e.to_string()))?,
        };

        Ok(BigInt(value))
    }
}

impl<'de> Deserialize<'de> for BigInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BigIntVisitor)
    }
}

impl Serialize for BigInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

struct IndexVisitor;

impl<'de> Visitor<'de> for IndexVisitor {
    type Value = Index;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string or integer for 4 byte index")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let without_prefix = value.trim_start_matches("0x");
        let val = u32::from_str_radix(without_prefix, 16).map_err(|e| E::custom(e.to_string()))?;

        Ok(Index(val))
    }

    fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Index(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let val: u32 = value
            .try_into()
            .map_err(|_| E::custom("index value doesn't fit 4 bytes"))?;

        Ok(Index(val))
    }
}

impl<'de> Deserialize<'de> for Index {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(IndexVisitor)
    }
}

impl Serialize for Index {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    macro_rules! impl_bytes_test {
        ($fn_name:ident, $kind:ident, $val:expr) => {
            #[test]
            fn $fn_name() {
                let data = $val;
                let bytes: $kind =
                    serde_json::from_value(JsonValue::String(data.to_owned())).unwrap();
                let real_bytes: Vec<u8> = prefix_hex::decode(data).unwrap();

                assert_eq!(bytes.0.as_slice(), real_bytes.as_slice());

                let serialized_data = serde_json::to_string(&bytes).unwrap();
                let deserialized_bytes: $kind = serde_json::from_str(&serialized_data).unwrap();

                assert_eq!(bytes, deserialized_bytes);
            }
        };
    }

    impl_bytes_test!(
        test_bytes32,
        Bytes32,
        "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
    );

    impl_bytes_test!(
        test_address,
        Address,
        "0xe65ef515adc3fd724c326adf8212b4284fd10137"
    );

    impl_bytes_test!(test_sighash, Sighash, "0xfb0f3ee1");

    impl_bytes_test!(
        test_bloomfilter_bytes,
        BloomFilterBytes,
        "0x5f3bd75b5dafa6b1dbbbfeff9d1d9ef7b1dbf7ddf95ed1bebff92c\
        fdf4bffb9370ff5572dedbcf7d57983f9be3bdc9be263ee6ec7b23bbdc\
        7f77fd7fffeef4bbacffebe47dd3bd7febdeaecffffccdf92fcf7ffbbd\
        6eb8e93ee6ff7feae4edb79f3d7feafbdfd7e7ffdfddeeb76cbbdbfe3fd\
        73ede3cffdefb2feffff96ae1e6eb3aa9a657fe1733fcf947bbf5d7f4d6\
        752eb5b7fbb0df9f1fffff526a1c7de39fdc677ffee9f9bf6eb3f7cbcb5\
        f4ece7e3fdb59f956ef83f7e77ea79b997f6c5feded6f5f8defffbf2b7f3\
        7fdceb7bc9fedbbdfd53f7dfd3dafff6ff8db6efb5fff7b6fde7b7fe9572\
        fdfe8e6df5cd6bdeff66af9de87efce1dd9bff7b777e5"
    );

    impl_bytes_test!(test_bytes_empty, Bytes, "0x");

    impl_bytes_test!(
        test_bytes,
        Bytes,
        "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9\
        352c97c6cacdcb6f31e65ef515adc3fd724c326adf8212b4284fd10137"
    );
}
