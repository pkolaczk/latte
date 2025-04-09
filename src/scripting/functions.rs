use crate::adapters::Adapters;
use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::context::Context;
use crate::scripting::cql_types::{Int8, Uuid};
use crate::scripting::Resources;
use chrono::Utc;
use metrohash::MetroHash64;
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rune::macros::{quote, MacroContext, TokenStream};
use rune::parse::Parser;
use rune::runtime::{Function, Mut, Ref, VmError, VmResult};
use rune::{ast, vm_try, Value};
use statrs::distribution::{Normal, Uniform};
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::ops::Deref;

use super::bind::to_aerospike_value;

/// Returns the literal value stored in the `params` map under the key given as the first
/// macro arg, and if not found, returns the expression from the second arg.
pub fn param(
    ctx: &mut MacroContext,
    params: &HashMap<String, String>,
    ts: &TokenStream,
) -> rune::compile::Result<TokenStream> {
    let mut parser = Parser::from_token_stream(ts, ctx.macro_span());
    let name = parser.parse::<ast::LitStr>()?;
    let name = ctx.resolve(name)?.to_string();
    let _ = parser.parse::<ast::Comma>()?;
    let expr = parser.parse::<ast::Expr>()?;
    let rhs = match params.get(&name) {
        Some(value) => {
            let src_id = ctx.insert_source(&name, value)?;
            let value = ctx.parse_source::<ast::Expr>(src_id)?;
            quote!(#value)
        }
        None => quote!(#expr),
    };
    Ok(rhs.into_token_stream(ctx)?)
}

/// Creates a new UUID for current iteration
#[rune::function]
pub fn uuid(i: i64) -> Uuid {
    Uuid::new(i)
}

#[rune::function]
pub fn float_to_i8(value: f64) -> Option<Int8> {
    Some(Int8((value as i64).try_into().ok()?))
}

/// Computes a hash of an integer value `i`.
/// Returns a value in range `0..i64::MAX`.
fn hash_inner(i: i64) -> i64 {
    let mut hash = MetroHash64::new();
    i.hash(&mut hash);
    (hash.finish() & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Computes a hash of an integer value `i`.
/// Returns a value in range `0..i64::MAX`.
#[rune::function]
pub fn hash(i: i64) -> i64 {
    hash_inner(i)
}

/// Computes hash of two integer values.
#[rune::function]
pub fn hash2(a: i64, b: i64) -> i64 {
    hash2_inner(a, b)
}

fn hash2_inner(a: i64, b: i64) -> i64 {
    let mut hash = MetroHash64::new();
    a.hash(&mut hash);
    b.hash(&mut hash);
    (hash.finish() & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Computes a hash of an integer value `i`.
/// Returns a value in range `0..max`.
#[rune::function]
pub fn hash_range(i: i64, max: i64) -> i64 {
    hash_inner(i) % max
}

/// Generates a floating point value with normal distribution
#[rune::function]
pub fn normal(i: i64, mean: f64, std_dev: f64) -> VmResult<f64> {
    let mut rng = SmallRng::seed_from_u64(i as u64);
    let distribution =
        vm_try!(Normal::new(mean, std_dev).map_err(|e| VmError::panic(format!("{e}"))));
    VmResult::Ok(distribution.sample(&mut rng))
}

#[rune::function]
pub fn uniform(i: i64, min: f64, max: f64) -> VmResult<f64> {
    let mut rng = SmallRng::seed_from_u64(i as u64);
    let distribution = vm_try!(Uniform::new(min, max).map_err(|e| VmError::panic(format!("{e}"))));
    VmResult::Ok(distribution.sample(&mut rng))
}

/// Generates random blob of data of given length.
/// Parameter `seed` is used to seed the RNG.
#[rune::function]
pub fn blob(seed: i64, len: usize) -> Vec<u8> {
    let mut rng = SmallRng::seed_from_u64(seed as u64);
    (0..len).map(|_| rng.gen::<u8>()).collect()
}

/// Generates random string of given length.
/// Parameter `seed` is used to seed
/// the RNG.
#[rune::function]
pub fn text(seed: i64, len: usize) -> String {
    let charset: Vec<char> = ("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".to_owned()
        + "0123456789!@#$%^&*()_+-=[]{}|;:',.<>?/")
        .chars()
        .collect();
    let mut rng = SmallRng::seed_from_u64(seed as u64);
    (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..charset.len());
            charset[idx]
        })
        .collect()
}

#[rune::function]
pub fn vector(len: usize, generator: Function) -> VmResult<Vec<Value>> {
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let value = vm_try!(generator.call((i,)));
        result.push(value);
    }
    VmResult::Ok(result)
}

/// Generates 'now' timestamp
#[rune::function]
pub fn now_timestamp() -> i64 {
    Utc::now().timestamp()
}

/// Selects one item from the collection based on the hash of the given value.
#[rune::function]
pub fn hash_select(i: i64, collection: &[Value]) -> Value {
    collection[(hash_inner(i) % collection.len() as i64) as usize].clone()
}

/// Joins all strings in vector with given separator
#[rune::function]
pub fn join(collection: &[Value], separator: &str) -> VmResult<String> {
    let mut result = String::new();
    let mut first = true;
    for v in collection {
        let v = vm_try!(v.clone().into_string());
        if !first {
            result.push_str(separator);
        }
        result.push_str(vm_try!(v.borrow_ref()).as_str());
        first = false;
    }
    VmResult::Ok(result)
}

/// Reads a file into a string.
#[rune::function]
pub fn read_to_string(filename: &str) -> io::Result<String> {
    let mut file = File::open(filename).expect("no such file");

    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;

    Ok(buffer)
}

/// Reads a file into a vector of lines.
#[rune::function]
pub fn read_lines(filename: &str) -> io::Result<Vec<String>> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    let result = buf
        .lines()
        .map(|l| l.expect("Could not parse line"))
        .collect();
    Ok(result)
}

/// Reads a file into a vector of words.
#[rune::function]
pub fn read_words(filename: &str) -> io::Result<Vec<String>> {
    let file = File::open(filename)
        .map_err(|e| io::Error::new(e.kind(), format!("Failed to open file {filename}: {e}")))?;
    let buf = BufReader::new(file);
    let mut result = Vec::new();
    for line in buf.lines() {
        let line = line?;
        let words = line
            .split(|c: char| !c.is_alphabetic())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());
        result.extend(words);
    }
    Ok(result)
}

/// Reads a resource file as a string.
fn read_resource_to_string_inner(path: &str) -> io::Result<String> {
    let resource = Resources::get(path).ok_or_else(|| {
        io::Error::new(ErrorKind::NotFound, format!("Resource not found: {path}"))
    })?;
    let contents = std::str::from_utf8(resource.data.as_ref())
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("Invalid UTF8 string: {e}")))?;
    Ok(contents.to_string())
}

#[rune::function]
pub fn read_resource_to_string(path: &str) -> io::Result<String> {
    read_resource_to_string_inner(path)
}

#[rune::function]
pub fn read_resource_lines(path: &str) -> io::Result<Vec<String>> {
    Ok(read_resource_to_string_inner(path)?
        .split('\n')
        .map(|s| s.to_string())
        .collect())
}

#[rune::function]
pub fn read_resource_words(path: &str) -> io::Result<Vec<String>> {
    Ok(read_resource_to_string_inner(path)?
        .split(|c: char| !c.is_alphabetic())
        .map(|s| s.to_string())
        .collect())
}

#[rune::function(instance)]
pub async fn prepare(mut ctx: Mut<Context>, key: Ref<str>, cql: Ref<str>) -> Result<(), CassError> {
    match ctx.adapter_mut() {
        Adapters::Scylla(ref mut sc) => sc.prepare(&key, &cql).await,
        Adapters::Postgres(ref mut sc) => sc.prepare(&key, &cql).await,
        _ => Err(CassError(CassErrorKind::Unsupported)),
    }
}

#[rune::function(instance)]
pub async fn execute(ctx: Ref<Context>, cql: Ref<str>) -> Result<(), CassError> {
    match ctx.adapter() {
        Adapters::Scylla(sc) => sc.execute(cql.deref()).await,
        Adapters::Postgres(sc) => sc.execute(cql.deref()).await,
        _ => Err(CassError(CassErrorKind::Unsupported)),
    }
}

#[rune::function(instance)]
pub async fn get(ctx: Ref<Context>, key: Ref<str>) -> Result<(), CassError> {
    match ctx.adapter() {
        Adapters::Aerospike(aero) => aero
            .get(key.deref())
            .await
            .map_err(|e| CassError(CassErrorKind::AerospikeError(e))),
        _ => Err(CassError(CassErrorKind::Unsupported)),
    }
}

#[rune::function(instance)]
pub async fn put(ctx: Ref<Context>, key: Ref<str>, value: Value) -> Result<(), CassError> {
    match ctx.adapter() {
        Adapters::Aerospike(aero) => aero
            .put(key.deref(), to_aerospike_value(value).unwrap())
            .await
            .map_err(|e| CassError(CassErrorKind::AerospikeError(e))),
        _ => Err(CassError(CassErrorKind::Unsupported)),
    }
}

#[rune::function(instance)]
pub async fn execute_prepared(
    ctx: Ref<Context>,
    key: Ref<str>,
    params: Value,
) -> Result<(), CassError> {
    match ctx.adapter() {
        Adapters::Scylla(sc) => sc.execute_prepared(&key, params).await,
        Adapters::Postgres(sc) => {
            let res = sc.execute_prepared(&key, params).await;
            if res.is_err() {
                let err = res.err().unwrap();
                print!("{}", err);

                return Err(err);
            }
            res
        }
        _ => Err(CassError(CassErrorKind::Unsupported)),
    }
}

#[rune::function(instance)]
pub async fn query_prepared(
    ctx: Ref<Context>,
    key: Ref<str>,
    params: Value,
) -> Result<(), CassError> {
    match ctx.adapter() {
        Adapters::Postgres(sc) => sc.query_prepared(&key, params).await,
        _ => Err(CassError(CassErrorKind::Unsupported)),
    }
}

#[rune::function(instance)]
pub fn elapsed_secs(ctx: &Context) -> f64 {
    ctx.elapsed_secs()
}
