use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryInto;
use std::error::Error as StdError;
use std::fmt;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::IsNull;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;

#[derive(Debug, Clone, derive_more::Deref, derive_more::From)]
pub struct Bytes32(pub Box<[u8; 32]>);

impl Bytes32 {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Box::new(bytes.try_into().unwrap()))
    }
}

impl ToSql for Bytes32 {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.as_slice().to_sql(ty, out)
    }
    fn accepts(ty: &Type) -> bool {
        <&[u8]>::accepts(ty)
    }
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.as_slice().to_sql_checked(ty, out)
    }
}

#[derive(Debug, Clone, derive_more::Deref, derive_more::From)]
pub struct Address(pub Box<[u8; 20]>);

impl Address {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Box::new(bytes.try_into().unwrap()))
    }
}

impl ToSql for Address {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.as_slice().to_sql(ty, out)
    }
    fn accepts(ty: &Type) -> bool {
        <&[u8]>::accepts(ty)
    }
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.as_slice().to_sql_checked(ty, out)
    }
}

#[derive(Debug, Clone, Copy, derive_more::Deref, derive_more::From)]
pub struct Nonce(pub u64);

impl Nonce {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(u64::from_be_bytes(bytes.try_into().unwrap()))
    }
}

impl ToSql for Nonce {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.to_be_bytes().as_slice().to_sql(ty, out)
    }
    fn accepts(ty: &Type) -> bool {
        <&[u8]>::accepts(ty)
    }
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.to_be_bytes().as_slice().to_sql_checked(ty, out)
    }
}

#[derive(Debug, Clone, derive_more::Deref, derive_more::From)]
pub struct BloomFilterBytes(pub Box<[u8; 256]>);

impl BloomFilterBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Box::new(bytes.try_into().unwrap()))
    }
}

impl ToSql for BloomFilterBytes {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.as_slice().to_sql(ty, out)
    }
    fn accepts(ty: &Type) -> bool {
        <&[u8]>::accepts(ty)
    }
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.as_slice().to_sql_checked(ty, out)
    }
}

#[derive(Debug, Clone, Copy, derive_more::Deref, derive_more::From)]
pub struct BigInt(pub i64);

impl ToSql for BigInt {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.to_sql(ty, out)
    }
    fn accepts(ty: &Type) -> bool {
        i64::accepts(ty)
    }
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.to_sql_checked(ty, out)
    }
}

#[derive(Debug, Clone, derive_more::Deref, derive_more::From)]
pub struct Bytes(pub Vec<u8>);

impl ToSql for Bytes {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.as_slice().to_sql(ty, out)
    }
    fn accepts(ty: &Type) -> bool {
        <&[u8]>::accepts(ty)
    }
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send + 'static>> {
        self.0.as_slice().to_sql_checked(ty, out)
    }
}

struct Bytes32Visitor;

impl<'de> Visitor<'de> for Bytes32Visitor {
    type Value = Bytes32;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string")
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
        let hex = prefix_hex::encode(&*self.0);

        serializer.serialize_str(&hex)
    }
}

struct AddressVisitor;

impl<'de> Visitor<'de> for AddressVisitor {
    type Value = Address;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string")
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
        let hex = prefix_hex::encode(&*self.0);

        serializer.serialize_str(&hex)
    }
}

struct NonceVisitor;

impl<'de> Visitor<'de> for NonceVisitor {
    type Value = Nonce;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let without_prefix = value.trim_start_matches("0x");
        let val = u64::from_str_radix(without_prefix, 16).map_err(|e| E::custom(e.to_string()))?;

        Ok(Nonce(val))
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
        serializer.serialize_u64(self.0)
    }
}

struct BloomFilterBytesVisitor;

impl<'de> Visitor<'de> for BloomFilterBytesVisitor {
    type Value = BloomFilterBytes;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string")
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
        let hex = prefix_hex::encode(&*self.0);

        serializer.serialize_str(&hex)
    }
}

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Bytes;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string")
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
        formatter.write_str("hex string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let without_prefix = value.trim_start_matches("0x");
        let val = i64::from_str_radix(without_prefix, 16).map_err(|e| E::custom(e.to_string()))?;

        Ok(BigInt(val))
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
        serializer.serialize_i64(self.0)
    }
}
