//! Functions for binding rune values to CQL parameters

use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::cql_types::Uuid;
use rune::{Any, ToValue, Value};
use scylla::cluster::metadata::{CollectionType, NativeType};
use scylla::frame::response::result::ColumnSpec;
use scylla::frame::response::result::ColumnType;
use scylla::value::{CqlTimeuuid, CqlValue};

use std::borrow::Cow;

use std::net::IpAddr;
use std::str::FromStr;

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
            Ok(CqlValue::Timestamp(scylla::value::CqlTimestamp(*v)))
        }

        (Value::Float(v), ColumnType::Native(NativeType::Float)) => Ok(CqlValue::Float(*v as f32)),
        (Value::Float(v), ColumnType::Native(NativeType::Double)) => Ok(CqlValue::Double(*v)),

        (Value::String(s), ColumnType::Native(NativeType::Timeuuid)) => {
            let timeuuid_str = s.borrow_ref().unwrap();
            let timeuuid = CqlTimeuuid::from_str(timeuuid_str.as_str());
            match timeuuid {
                Ok(timeuuid) => Ok(CqlValue::Timeuuid(timeuuid)),
                Err(e) => Err(CassError::new(CassErrorKind::QueryParamConversion(
                    format!("{:?}", v),
                    ColumnType::Native(NativeType::Timeuuid),
                    Some(format!("{}", e)),
                ))),
            }
        }
        (Value::String(v), ColumnType::Native(NativeType::Text | NativeType::Ascii)) => {
            Ok(CqlValue::Text(v.borrow_ref().unwrap().as_str().to_string()))
        }
        (Value::String(s), ColumnType::Native(NativeType::Inet)) => {
            let ipaddr_str = s.borrow_ref().unwrap();
            let ipaddr = IpAddr::from_str(ipaddr_str.as_str());
            match ipaddr {
                Ok(ipaddr) => Ok(CqlValue::Inet(ipaddr)),
                Err(e) => Err(CassError::new(CassErrorKind::QueryParamConversion(
                    format!("{:?}", v),
                    ColumnType::Native(NativeType::Inet),
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

        // lists
        (
            Value::Vec(v),
            ColumnType::Collection {
                typ: CollectionType::List(elt),
                ..
            },
        ) => {
            let v = v.borrow_ref().unwrap();
            let mut elements = Vec::with_capacity(v.len());
            for elem in v.iter() {
                let elem = to_scylla_value(elem, elt)?;
                elements.push(elem);
            }
            Ok(CqlValue::List(elements))
        }

        // sets
        (
            Value::Vec(v),
            ColumnType::Collection {
                typ: CollectionType::Set(elt),
                ..
            },
        ) => {
            let v = v.borrow_ref().unwrap();
            let mut elements = Vec::with_capacity(v.len());
            for elem in v.iter() {
                let elem = to_scylla_value(elem, elt)?;
                elements.push(elem);
            }
            Ok(CqlValue::Set(elements))
        }

        // maps
        (
            Value::Vec(v),
            ColumnType::Collection {
                typ: CollectionType::Map(key_elt, value_elt),
                ..
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
                        return Err(CassError::new(CassErrorKind::QueryParamConversion(
                            format!("{:?}", tuple),
                            ColumnType::Tuple(vec![
                                key_elt.as_ref().clone().into_owned(),
                                value_elt.as_ref().clone().into_owned(),
                            ]),
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
                typ: CollectionType::Map(key_elt, value_elt),
                ..
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

        // Vector
        (Value::Vec(v), ColumnType::Vector { typ, .. }) => {
            let v = v.borrow_ref().unwrap();
            let mut elements = Vec::with_capacity(v.len());
            for elem in v.iter() {
                let elem = to_scylla_value(elem, typ)?;
                elements.push(elem);
            }
            Ok(CqlValue::Vector(elements))
        }

        // UDTs
        (Value::Object(v), ColumnType::UserDefinedType { definition, .. }) => {
            let obj = v.borrow_ref().unwrap();
            let field_types = &definition.field_types;
            let fields = read_fields(|s| obj.get(s), field_types)?;
            Ok(CqlValue::UserDefinedType {
                keyspace: definition.keyspace.to_string(),
                name: definition.name.to_string(),
                fields,
            })
        }
        (Value::Struct(v), ColumnType::UserDefinedType { definition, .. }) => {
            let obj = v.borrow_ref().unwrap();
            let field_types = &definition.field_types;
            let fields = read_fields(|s| obj.get(s), field_types)?;
            Ok(CqlValue::UserDefinedType {
                keyspace: definition.keyspace.to_string(),
                name: definition.name.to_string(),
                fields,
            })
        }

        (Value::Any(obj), ColumnType::Native(NativeType::Uuid)) => {
            let obj = obj.borrow_ref().unwrap();
            let h = obj.type_hash();
            if h == Uuid::type_hash() {
                let uuid: &Uuid = obj.downcast_borrow_ref().unwrap();
                Ok(CqlValue::Uuid(uuid.0))
            } else {
                Err(CassError::new(CassErrorKind::QueryParamConversion(
                    format!("{:?}", v),
                    ColumnType::Native(NativeType::Uuid),
                    None,
                )))
            }
        }
        (value, typ) => Err(CassError::new(CassErrorKind::QueryParamConversion(
            format!("{:?}", value),
            typ.clone().into_owned(),
            None,
        ))),
    }
}

fn convert_int<T: TryFrom<i64>, R>(
    value: i64,
    typ: ColumnType,
    f: impl Fn(T) -> R,
) -> Result<R, CassError> {
    let converted = value.try_into().map_err(|_| {
        CassError::new(CassErrorKind::ValueOutOfRange(
            value.to_string(),
            typ.clone().into_owned(),
        ))
    })?;
    Ok(f(converted))
}

/// Binds parameters passed as a single rune value to the arguments of the statement.
/// The `params` value can be a tuple, a vector, a struct or an object.
pub fn to_scylla_query_params(
    params: &Value,
    types: &[ColumnSpec],
) -> Result<Vec<CqlValue>, CassError> {
    Ok(match params {
        Value::Tuple(tuple) => {
            let mut values = Vec::new();
            let tuple = tuple.borrow_ref().unwrap();
            if tuple.len() != types.len() {
                return Err(CassError::new(CassErrorKind::InvalidNumberOfQueryParams));
            }
            for (v, t) in tuple.iter().zip(types) {
                values.push(to_scylla_value(v, t.typ())?);
            }
            values
        }
        Value::Vec(vec) => {
            let mut values = Vec::new();

            let vec = vec.borrow_ref().unwrap();
            for (v, t) in vec.iter().zip(types) {
                values.push(to_scylla_value(v, t.typ())?);
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
            return Err(CassError::new(CassErrorKind::InvalidQueryParamsObject(
                other.type_info().unwrap(),
            )));
        }
    })
}

fn read_params<'a, 'b>(
    get_value: impl Fn(&str) -> Option<&'a Value>,
    params: &[ColumnSpec],
) -> Result<Vec<CqlValue>, CassError> {
    let mut values = Vec::with_capacity(params.len());
    for column in params {
        let value = match get_value(column.name()) {
            Some(value) => to_scylla_value(value, column.typ())?,
            None => CqlValue::Empty,
        };
        values.push(value)
    }
    Ok(values)
}

fn read_fields<'a, 'b>(
    get_value: impl Fn(&str) -> Option<&'a Value>,
    fields: &[(Cow<str>, ColumnType)],
) -> Result<Vec<(String, Option<CqlValue>)>, CassError> {
    let mut values = Vec::with_capacity(fields.len());
    for (field_name, field_type) in fields {
        if let Some(value) = get_value(field_name) {
            let value = Some(to_scylla_value(value, field_type)?);
            values.push((field_name.to_string(), value))
        };
    }
    Ok(values)
}
