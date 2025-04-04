use metrohash::MetroHash128;
use rune::alloc::fmt::TryWrite;
use rune::runtime::VmResult;
use rune::{vm_write, Any};
use std::hash::Hash;
use uuid::{Variant, Version};

#[derive(Clone, Debug, Any)]
pub struct Int8(pub i8);

#[derive(Clone, Debug, Any)]
pub struct Int16(pub i16);

#[derive(Clone, Debug, Any)]
pub struct Int32(pub i32);

#[derive(Clone, Debug, Any)]
pub struct Float32(pub f32);

#[derive(Clone, Debug, Any)]
pub struct Uuid(pub uuid::Uuid);

impl Uuid {
    pub fn new(i: i64) -> Uuid {
        let mut hash = MetroHash128::new();
        i.hash(&mut hash);
        let (h1, h2) = hash.finish128();
        let h = ((h1 as u128) << 64) | (h2 as u128);
        let mut builder = uuid::Builder::from_u128(h);
        builder.set_variant(Variant::RFC4122);
        builder.set_version(Version::Random);
        Uuid(builder.into_uuid())
    }

    #[rune::function(protocol = STRING_DISPLAY)]
    pub fn string_display(&self, f: &mut rune::runtime::Formatter) -> VmResult<()> {
        vm_write!(f, "{}", self.0);
        VmResult::Ok(())
    }
}

#[derive(Clone, Any)]
pub struct Bin(pub aerospike::Bin);

impl Bin {
    pub fn new(name: String, value: String) -> Self {
        Self(aerospike::Bin::new(name, aerospike::Value::String(value)))
    }
}

pub mod i64 {
    use crate::scripting::cql_types::{Float32, Int16, Int32, Int8};

    /// Converts a Rune integer to i8 (Cassandra tinyint)
    #[rune::function(instance)]
    pub fn to_i8(value: i64) -> Option<Int8> {
        Some(Int8(value.try_into().ok()?))
    }

    /// Converts a Rune integer to i16 (Cassandra smallint)
    #[rune::function(instance)]
    pub fn to_i16(value: i64) -> Option<Int16> {
        Some(Int16(value.try_into().ok()?))
    }

    /// Converts a Rune integer to i32 (Cassandra int)
    #[rune::function(instance)]
    pub fn to_i32(value: i64) -> Option<Int32> {
        Some(Int32(value.try_into().ok()?))
    }

    /// Converts a Rune integer to f32 (Cassandra float)
    #[rune::function(instance)]
    pub fn to_f32(value: i64) -> Float32 {
        Float32(value as f32)
    }

    /// Converts a Rune integer to a String
    #[rune::function(instance)]
    pub fn to_string(value: i64) -> String {
        value.to_string()
    }

    /// Restricts a value to a certain interval.
    #[rune::function(instance)]
    pub fn clamp(value: i64, min: i64, max: i64) -> i64 {
        value.clamp(min, max)
    }
}

pub mod f64 {
    use crate::scripting::cql_types::{Float32, Int16, Int32, Int8};

    #[rune::function(instance)]
    pub fn to_i8(value: f64) -> Int8 {
        Int8(value as i8)
    }

    #[rune::function(instance)]
    pub fn to_i16(value: f64) -> Int16 {
        Int16(value as i16)
    }

    #[rune::function(instance)]
    pub fn to_i32(value: f64) -> Int32 {
        Int32(value as i32)
    }

    #[rune::function(instance)]
    pub fn to_f32(value: f64) -> Float32 {
        Float32(value as f32)
    }

    #[rune::function(instance)]
    pub fn to_string(value: f64) -> String {
        value.to_string()
    }

    /// Restricts a value to a certain interval unless it is NaN.
    #[rune::function(instance)]
    pub fn clamp(value: f64, min: f64, max: f64) -> f64 {
        value.clamp(min, max)
    }
}
