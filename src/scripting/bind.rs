//! Functions for binding rune values to CQL parameters

use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::cql_types::Uuid;
use aerospike::Bin;
use itertools::*;
use rune::alloc::borrow::TryToOwned;
use rune::runtime::Object;
use rune::{Any, ToValue, Value};
use scylla::_macro_internal::ColumnType;
use scylla::cluster::metadata::{CollectionType, NativeType, UserDefinedType};
use scylla::response::query_result::ColumnSpecs;
use scylla::value::{CqlTimestamp, CqlTimeuuid, CqlValue};
use std::borrow::Cow;
use std::net::IpAddr;
use std::str::FromStr;
use tokio_postgres::types::{ToSql, Type};

fn to_scylla_value(v: &Value, typ: &ColumnType) -> Result<CqlValue, CassError> {
    // TODO: add support for the following native CQL types:
    //       'counter', 'date', 'decimal', 'duration', 'time' and 'variant'.
    //       Also, for the 'tuple'.
    match (v, typ) {
        (Value::Bool(v), ColumnType::Native(NativeType::Boolean)) => Ok(CqlValue::Boolean(*v)),

        (Value::Byte(v), ColumnType::Native(NativeType::TinyInt)) => {
            Ok(CqlValue::TinyInt(*v as i8))
        }
        (Value::Byte(v), ColumnType::Native(NativeType::SmallInt)) => {
            Ok(CqlValue::SmallInt(*v as i16))
        }
        (Value::Byte(v), ColumnType::Native(NativeType::Int)) => Ok(CqlValue::Int(*v as i32)),
        (Value::Byte(v), ColumnType::Native(NativeType::BigInt)) => Ok(CqlValue::BigInt(*v as i64)),

        (Value::Integer(v), ColumnType::Native(NativeType::TinyInt)) => convert_int(
            *v,
            ColumnType::Native(NativeType::TinyInt),
            CqlValue::TinyInt,
        ),
        (Value::Integer(v), ColumnType::Native(NativeType::SmallInt)) => convert_int(
            *v,
            ColumnType::Native(NativeType::SmallInt),
            CqlValue::SmallInt,
        ),
        (Value::Integer(v), ColumnType::Native(NativeType::Int)) => {
            convert_int(*v, ColumnType::Native(NativeType::Int), CqlValue::Int)
        }
        (Value::Integer(v), ColumnType::Native(NativeType::BigInt)) => Ok(CqlValue::BigInt(*v)),
        (Value::Integer(v), ColumnType::Native(NativeType::Timestamp)) => {
            Ok(CqlValue::Timestamp(CqlTimestamp(*v)))
        }

        (Value::Float(v), ColumnType::Native(NativeType::Float)) => Ok(CqlValue::Float(*v as f32)),
        (Value::Float(v), ColumnType::Native(NativeType::Double)) => Ok(CqlValue::Double(*v)),

        (Value::String(s), ColumnType::Native(NativeType::Timeuuid)) => {
            let timeuuid_str = s.borrow_ref().unwrap();
            let timeuuid = CqlTimeuuid::from_str(timeuuid_str.as_str());
            match timeuuid {
                Ok(timeuuid) => Ok(CqlValue::Timeuuid(timeuuid)),
                Err(e) => Err(CassError(CassErrorKind::QueryParamConversion(
                    format!("{:?}", v),
                    Some(format!("{}", e)),
                ))),
            }
        }
        (
            Value::String(v),
            ColumnType::Native(NativeType::Text) | ColumnType::Native(NativeType::Ascii),
        ) => Ok(CqlValue::Text(v.borrow_ref().unwrap().as_str().to_string())),
        (Value::String(s), ColumnType::Native(NativeType::Inet)) => {
            let ipaddr_str = s.borrow_ref().unwrap();
            let ipaddr = IpAddr::from_str(ipaddr_str.as_str());
            match ipaddr {
                Ok(ipaddr) => Ok(CqlValue::Inet(ipaddr)),
                Err(e) => Err(CassError(CassErrorKind::QueryParamConversion(
                    format!("{:?}", v),
                    Some(format!("{}", e)),
                ))),
            }
        }
        (Value::Bytes(v), ColumnType::Native(NativeType::Blob)) => {
            Ok(CqlValue::Blob(v.borrow_ref().unwrap().to_vec()))
        }
        (Value::Vec(v), ColumnType::Native(NativeType::Blob)) => {
            let v: Vec<Value> = v.borrow_ref().unwrap().to_vec();
            let byte_vec: Vec<u8> = v
                .into_iter()
                .map(|value| value.as_byte().unwrap())
                .collect();
            Ok(CqlValue::Blob(byte_vec))
        }
        (Value::Option(v), typ) => match v.borrow_ref().unwrap().as_ref() {
            Some(v) => to_scylla_value(v, typ),
            None => Ok(CqlValue::Empty),
        },
        (
            Value::Vec(v),
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(elt),
            },
        ) => {
            let v = v.borrow_ref().unwrap();
            let elements = v
                .as_ref()
                .iter()
                .map(|v| to_scylla_value(v, elt))
                .try_collect()?;
            Ok(CqlValue::List(elements))
        }
        (
            Value::Vec(v),
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(elt),
            },
        ) => {
            let v = v.borrow_ref().unwrap();
            let elements = v
                .as_ref()
                .iter()
                .map(|v| to_scylla_value(v, elt))
                .try_collect()?;
            Ok(CqlValue::Set(elements))
        }
        (
            Value::Vec(v),
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(key_elt, value_elt),
            },
        ) => {
            let v = v.borrow_ref().unwrap();
            let mut map_vec = Vec::with_capacity(v.len());
            for tuple in v.iter() {
                match tuple {
                    Value::Tuple(tuple) if tuple.borrow_ref().unwrap().len() == 2 => {
                        let tuple = tuple.borrow_ref().unwrap();
                        let key = to_scylla_value(tuple.first().unwrap(), key_elt)?;
                        let value = to_scylla_value(tuple.get(1).unwrap(), value_elt)?;
                        map_vec.push((key, value));
                    }
                    _ => {
                        return Err(CassError(CassErrorKind::QueryParamConversion(
                            format!("{:?}", tuple),
                            None,
                        )));
                    }
                }
            }
            Ok(CqlValue::Map(map_vec))
        }
        (
            Value::Object(obj),
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(key_elt, value_elt),
            },
        ) => {
            let obj = obj.borrow_ref().unwrap();
            let mut map_vec = Vec::with_capacity(obj.keys().len());
            for (k, v) in obj.iter() {
                let key = String::from(k.as_str());
                let key = to_scylla_value(&(key.to_value().unwrap()), key_elt)?;
                let value = to_scylla_value(v, value_elt)?;
                map_vec.push((key, value));
            }
            Ok(CqlValue::Map(map_vec))
        }

        (Value::Any(obj), ColumnType::Native(NativeType::Uuid)) => {
            let obj = obj.borrow_ref().unwrap();
            let h = obj.type_hash();
            if h == Uuid::type_hash() {
                let uuid: &Uuid = obj.downcast_borrow_ref().unwrap();
                Ok(CqlValue::Uuid(uuid.0))
            } else {
                Err(CassError(CassErrorKind::QueryParamConversion(
                    format!("{:?}", v),
                    None,
                )))
            }
        }
        (
            val,
            ColumnType::UserDefinedType {
                frozen: _f,
                definition,
            },
        ) => {
            let non_arc_definition = UserDefinedType {
                keyspace: definition.keyspace.clone(),
                name: definition.name.clone(),
                field_types: definition.field_types.clone(),
            };
            match (val, non_arc_definition) {
                (
                    Value::Struct(v),
                    UserDefinedType {
                        keyspace,
                        name: type_name,
                        field_types,
                    },
                ) => {
                    let obj = v.borrow_ref().unwrap();
                    let fields = read_fields(|s| obj.get(s), &field_types)?;
                    Ok(CqlValue::UserDefinedType {
                        keyspace: keyspace.to_string(),
                        name: type_name.to_string(),
                        fields,
                    })
                }
                (
                    Value::Object(v),
                    UserDefinedType {
                        keyspace,
                        name: type_name,
                        field_types,
                    },
                ) => {
                    let obj = v.borrow_ref().unwrap();
                    let fields = read_fields(|s| obj.get(s), &field_types)?;
                    Ok(CqlValue::UserDefinedType {
                        keyspace: keyspace.to_string(),
                        name: type_name.to_string(),
                        fields,
                    })
                }
                (value, _) => Err(CassError(CassErrorKind::QueryParamConversion(
                    format!("{:?}", value),
                    None,
                ))),
            }
        }
        (value, _) => Err(CassError(CassErrorKind::QueryParamConversion(
            format!("{:?}", value),
            None,
        ))),
    }
}

fn to_pg_value(v: &Value, typ: &Type) -> Result<Box<dyn ToSql + Sync>, CassError> {
    // TODO: support vec
    match (v, typ.name()) {
        (Value::Bool(v), "bool") => Ok(Box::new(*v)),
        (Value::Byte(v), "char") => Ok(Box::new(*v as i8)),
        (Value::Integer(v), "char") => Ok(Box::new(*v as i8)),
        (Value::Integer(v), "int8") => Ok(Box::new(*v)),
        (Value::Integer(v), "int4") => Ok(Box::new(*v as i32)),
        (Value::Integer(v), "int2") => Ok(Box::new(*v as i16)),
        (Value::Integer(v), "oid") => Ok(Box::new(*v as u32)),
        (Value::Float(v), "float4") => Ok(Box::new(*v as f32)),
        (Value::Float(v), "float8") => Ok(Box::new(*v)),
        (Value::String(v), "text") => Ok(Box::new(v.borrow_ref().unwrap().to_string())),
        (Value::String(v), "varchar") => Ok(Box::new(v.borrow_ref().unwrap().to_string())),
        (Value::Bytes(v), "bytea") => Ok(Box::new(v.borrow_ref().unwrap().to_vec())),
        (value, _) => Err(CassError(CassErrorKind::QueryParamConversion(
            format!("{:?}", value),
            Some(typ.name().to_string()),
        ))),
    }
}

pub fn to_aerospike_value(v: Value) -> Result<Vec<Bin>, CassError> {
    match v {
        Value::Vec(vec) => {
            let (correct_bins, errors): (Vec<Result<Bin, CassError>>, Vec<Result<Bin, CassError>>) =
                vec.borrow_ref()
                    .unwrap()
                    .to_vec()
                    .into_iter()
                    .map(|v| {
                        rune::from_value(v)
                            .map_err(|e| {
                                CassError(CassErrorKind::QueryParamConversion(
                                    format!("Failed to convert entry to Object: {:?}", e,),
                                    None,
                                ))
                            })
                            .map(object_to_bin)
                            .and_then(|i| i)
                    })
                    .partition(|x| x.is_ok());

            if !errors.is_empty() {
                return Err(CassError(CassErrorKind::QueryParamConversion(
                    format!(
                        "Failed to map data. Errors: {:?}",
                        errors
                            .into_iter()
                            .map(|e| format!("{:?}", e.err()))
                            .join(", ")
                    ),
                    None,
                )));
            }

            Ok(correct_bins.into_iter().map(|x| x.unwrap()).collect_vec())
        }
        Value::Object(obj) => {
            let object = obj.borrow_ref().unwrap().try_to_owned().map_err(|e| {
                CassError(CassErrorKind::QueryParamConversion(
                    format!("{:?}", obj),
                    Some(format!("{:?}", e)),
                ))
            })?;
            let bin = object_to_bin(object)?;

            Ok(vec![bin])
        }
        value => Err(CassError(CassErrorKind::QueryParamConversion(
            format!("{:?}", value),
            None,
        ))),
    }
}

fn object_to_bin(object: Object) -> Result<Bin, CassError> {
    let name = object.get("name");
    let value = object.get("value");
    match (name, value) {
        (Some(Value::String(name)), Some(value)) => {
            let name = name.borrow_ref().unwrap().to_string();
            match value {
                Value::String(v) => Ok(Bin::new(
                    name,
                    aerospike::Value::String(v.borrow_ref().unwrap().to_string()),
                )),
                Value::Float(v) => Ok(Bin::new(
                    name,
                    aerospike::Value::Float(aerospike::FloatValue::from(v.to_owned())),
                )),
                Value::Bool(v) => Ok(Bin::new(name, aerospike::Value::Bool(v.to_owned()))),
                Value::Byte(v) => Ok(Bin::new(name, aerospike::Value::Blob(vec![v.to_owned()]))),
                Value::Bytes(v) => Ok(Bin::new(
                    name,
                    aerospike::Value::Blob(v.borrow_ref().unwrap().to_vec()),
                )),
                _ => Err(CassError(CassErrorKind::QueryParamConversion(
                    format!("Unsupported value for bin: {:?}, {:?}", name, value),
                    None,
                ))),
            }
        }
        _ => Err(CassError(CassErrorKind::QueryParamConversion(
            format!("Invalid bin: {:?}, {:?}", name, value),
            None,
        ))),
    }
}

fn convert_int<T: TryFrom<i64>, R>(
    value: i64,
    _typ: ColumnType,
    f: impl Fn(T) -> R,
) -> Result<R, CassError> {
    let converted = value
        .try_into()
        .map_err(|_| CassError(CassErrorKind::ValueOutOfRange(value.to_string())))?;
    Ok(f(converted))
}

/// Binds parameters passed as a single rune value to the arguments of the statement.
/// The `params` value can be a tuple, a vector, a struct or an object.
pub fn to_scylla_query_params(
    params: &Value,
    types: ColumnSpecs,
) -> Result<Vec<CqlValue>, CassError> {
    Ok(match params {
        Value::Tuple(tuple) => {
            let mut values = Vec::new();
            let tuple = tuple.borrow_ref().unwrap();
            if tuple.len() != types.len() {
                return Err(CassError(CassErrorKind::InvalidNumberOfQueryParams));
            }
            for (v, t) in tuple.iter().zip(types.as_slice()) {
                values.push(to_scylla_value(v, &t.typ())?);
            }
            values
        }
        Value::Vec(vec) => {
            let mut values = Vec::new();

            let vec = vec.borrow_ref().unwrap();
            for (v, t) in vec.iter().zip(types.as_slice()) {
                values.push(to_scylla_value(v, &t.typ())?);
            }
            values
        }
        Value::Object(obj) => {
            let obj = obj.borrow_ref().unwrap();
            read_params(|f| obj.get(f), types)?
        }
        Value::Struct(obj) => {
            let obj = obj.borrow_ref().unwrap();
            read_params(|f| obj.get(f), types)?
        }
        other => {
            return Err(CassError(CassErrorKind::InvalidQueryParamsObject(
                other.type_info().unwrap(),
            )));
        }
    })
}

/// Binds parameters passed as a single rune value to the arguments of the statement.
/// The `params` value can be a tuple, a vector, a struct or an object.
pub fn to_pg_query_params(
    params: &Value,
    types: &[Type],
) -> Result<Vec<Box<dyn ToSql + Sync>>, CassError> {
    Ok(match params {
        Value::Tuple(tuple) => {
            let mut values = Vec::new();
            let tuple = tuple.borrow_ref().unwrap();
            if tuple.len() != types.len() {
                return Err(CassError(CassErrorKind::InvalidNumberOfQueryParams));
            }
            for (v, t) in tuple.iter().zip(types) {
                values.push(to_pg_value(v, &t)?);
            }
            values
        }
        Value::Vec(vec) => {
            let mut values = Vec::new();

            let vec = vec.borrow_ref().unwrap();
            for (v, t) in vec.iter().zip(types) {
                values.push(to_pg_value(v, &t)?);
            }
            values
        }
        other => {
            return Err(CassError(CassErrorKind::InvalidQueryParamsObject(
                other.type_info().unwrap(),
            )));
        }
    })
}

fn read_params<'a, 'b>(
    get_value: impl Fn(&str) -> Option<&'a Value>,
    params: ColumnSpecs,
) -> Result<Vec<CqlValue>, CassError> {
    let mut values = Vec::with_capacity(params.len());
    for column in params.as_slice() {
        let value = match get_value(&column.name()) {
            Some(value) => to_scylla_value(value, &column.typ())?,
            None => CqlValue::Empty,
        };
        values.push(value)
    }
    Ok(values)
}

fn read_fields<'a, 'b>(
    get_value: impl Fn(&str) -> Option<&'a Value>,
    fields: &Vec<(Cow<str>, ColumnType)>,
) -> Result<Vec<(String, Option<CqlValue>)>, CassError> {
    let mut values = Vec::with_capacity(fields.len());
    for (field_name, field_type) in fields {
        if let Some(v) = get_value(field_name) {
            values.push((
                field_name.to_string(),
                Some(to_scylla_value(v, field_type)?),
            ))
        };
    }
    Ok(values)
}
