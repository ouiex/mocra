#![allow(unused)]
use calamine::{Data, DataType};
use polars::prelude::*;
use serde_json::Value;
#[inline]
fn is_null_like(s: &str) -> bool {
    let trimmed = s.trim();
    trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null")
}

#[inline]
fn as_bool_like(s: &str) -> Option<bool> {
    let trimmed = s.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        Some(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

#[inline]
pub fn json_value_to_any_value(values: &'_ [Value]) -> Vec<AnyValue<'_>> {
    // First pass: detect the most suitable uniform target type.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Target {
        Utf8,
        Boolean,
        Float64,
        Int64,
        UInt64,
        // All Nulls will still be represented as Null AnyValues, regardless of target
    }

    let mut has_non_bool_string = false;
    let mut has_bool = false;
    let mut has_number = false;
    let mut has_float = false;
    let mut has_i64 = false;
    let mut has_u64 = false;

    // Early-exit to Utf8 if we see any array/object or a non-bool string,
    // or a mix of booleans and numbers.
    for v in values.iter() {
        match v {
            Value::Null => {}
            Value::Bool(_) => {
                if has_number {
                    // mix of bool and number -> Utf8
                    has_non_bool_string = true; // force Utf8 choice
                    break;
                }
                has_bool = true;
            }
            Value::Number(n) => {
                if has_bool {
                    has_non_bool_string = true; // force Utf8
                    break;
                }
                has_number = true;
                if n.is_f64() {
                    has_float = true;
                } else if n.is_i64() {
                    has_i64 = true;
                } else if n.is_u64() {
                    has_u64 = true;
                }
            }
            Value::String(s) => {
                // cache checks to avoid duplicate work
                let bool_like = as_bool_like(s);
                let null_like = is_null_like(s);
                if bool_like.is_some() || null_like {
                    if bool_like.is_some() {
                        has_bool = true;
                    }
                } else {
                    has_non_bool_string = true;
                    break;
                }
            }
            Value::Array(_) | Value::Object(_) => {
                has_non_bool_string = true;
                break;
            }
        }
    }

    // Decide on target
    let target = if has_non_bool_string || (has_number && has_bool) {
        Target::Utf8
    } else if has_number {
        if has_float || (has_i64 && has_u64) {
            Target::Float64
        } else if has_i64 {
            Target::Int64
        } else {
            Target::UInt64
        }
    } else if has_bool {
        Target::Boolean
    } else {
        // all nulls or empty strings -> keep as Nulls; Utf8 is fine as well but Nulls are uniform
        // we'll still return Null AnyValues below
        Target::Utf8
    };

    // Second pass: convert values to the chosen target, allowing Nulls.
    let mut out = Vec::with_capacity(values.len());
    for v in values.iter() {
        let any = match target {
            Target::Utf8 => match v {
                Value::Null => AnyValue::Null,
                Value::Bool(b) => AnyValue::StringOwned(b.to_string().into()),
                Value::Number(n) => AnyValue::StringOwned(n.to_string().into()),
                Value::String(s) => {
                    if is_null_like(s) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(s.clone().into())
                    }
                }
                Value::Array(_) | Value::Object(_) => AnyValue::StringOwned(v.to_string().into()),
            },
            Target::Boolean => match v {
                Value::Bool(b) => AnyValue::Boolean(*b),
                Value::String(s) => match as_bool_like(s) {
                    Some(b) => AnyValue::Boolean(b),
                    None => AnyValue::Null,
                },
                _ => AnyValue::Null,
            },
            Target::Float64 => match v {
                Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        AnyValue::Float64(f)
                    } else if let Some(i) = n.as_i64() {
                        AnyValue::Float64(i as f64)
                    } else if let Some(u) = n.as_u64() {
                        AnyValue::Float64(u as f64)
                    } else {
                        AnyValue::Null
                    }
                }
                Value::Null => AnyValue::Null,
                Value::String(s) => {
                    if is_null_like(s) {
                        AnyValue::Null
                    } else {
                        // shouldn't happen due to detection; keep Null to avoid type pollution
                        AnyValue::Null
                    }
                }
                _ => AnyValue::Null,
            },
            Target::Int64 => match v {
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        AnyValue::Int64(i)
                    } else {
                        // Fallbacks should be rare due to detection
                        AnyValue::Null
                    }
                }
                Value::Null => AnyValue::Null,
                Value::String(s) => {
                    if is_null_like(s) {
                        AnyValue::Null
                    } else {
                        AnyValue::Null
                    }
                }
                _ => AnyValue::Null,
            },
            Target::UInt64 => match v {
                Value::Number(n) => {
                    if let Some(u) = n.as_u64() {
                        AnyValue::UInt64(u)
                    } else {
                        AnyValue::Null
                    }
                }
                Value::Null => AnyValue::Null,
                Value::String(s) => {
                    if is_null_like(s) {
                        AnyValue::Null
                    } else {
                        AnyValue::Null
                    }
                }
                _ => AnyValue::Null,
            },
        };
        out.push(any);
    }
    out
}

/// 更高效的构建：直接用 typed 容器构建 Series，避免 AnyValue 中转。
#[inline]
pub fn serde_values_to_series(name: &str, values: &[Value]) -> Series {
    // 与 json_value_to_any_value 相同的目标类型判定
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Target {
        Utf8,
        Boolean,
        Float64,
        Int64,
        UInt64,
    }

    let mut has_non_bool_string = false;
    let mut has_bool = false;
    let mut has_number = false;
    let mut has_float = false;
    let mut has_i64 = false;
    let mut has_u64 = false;

    for v in values.iter() {
        match v {
            Value::Null => {}
            Value::Bool(_) => {
                if has_number {
                    has_non_bool_string = true;
                    break;
                }
                has_bool = true;
            }
            Value::Number(n) => {
                if has_bool {
                    has_non_bool_string = true;
                    break;
                }
                has_number = true;
                if n.is_f64() {
                    has_float = true;
                } else if n.is_i64() {
                    has_i64 = true;
                } else if n.is_u64() {
                    has_u64 = true;
                }
            }
            Value::String(s) => {
                let bool_like = as_bool_like(s);
                let null_like = is_null_like(s);
                if bool_like.is_some() || null_like {
                    if bool_like.is_some() {
                        has_bool = true;
                    }
                } else {
                    has_non_bool_string = true;
                    break;
                }
            }
            Value::Array(_) | Value::Object(_) => {
                has_non_bool_string = true;
                break;
            }
        }
    }

    let target = if has_non_bool_string || (has_number && has_bool) {
        Target::Utf8
    } else if has_number {
        if has_float || (has_i64 && has_u64) {
            Target::Float64
        } else if has_i64 {
            Target::Int64
        } else {
            Target::UInt64
        }
    } else if has_bool {
        Target::Boolean
    } else {
        Target::Utf8
    };

    match target {
        Target::Utf8 => {
            let mut buf: Vec<Option<String>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Null => buf.push(None),
                    Value::Bool(b) => buf.push(Some(b.to_string())),
                    Value::Number(n) => buf.push(Some(n.to_string())),
                    Value::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(Some(s.clone()))
                        }
                    }
                    Value::Array(_) | Value::Object(_) => buf.push(Some(v.to_string())),
                }
            }
            Series::new(name.into(), buf)
        }
        Target::Boolean => {
            let mut buf: Vec<Option<bool>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Bool(b) => buf.push(Some(*b)),
                    Value::String(s) => buf.push(as_bool_like(s)),
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        Target::Float64 => {
            let mut buf: Vec<Option<f64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            buf.push(Some(f))
                        } else if let Some(i) = n.as_i64() {
                            buf.push(Some(i as f64))
                        } else if let Some(u) = n.as_u64() {
                            buf.push(Some(u as f64))
                        } else {
                            buf.push(None)
                        }
                    }
                    Value::Null => buf.push(None),
                    Value::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        Target::Int64 => {
            let mut buf: Vec<Option<i64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Number(n) => buf.push(n.as_i64()),
                    Value::Null => buf.push(None),
                    Value::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        Target::UInt64 => {
            let mut buf: Vec<Option<u64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Number(n) => buf.push(n.as_u64()),
                    Value::Null => buf.push(None),
                    Value::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
    }
}

/// 显式指定目标类型，跳过类型探测，适合已知 schema 的高性能场景。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonTarget {
    Utf8,
    Boolean,
    Float64,
    Int64,
    UInt64,
}

#[inline]
pub fn json_values_to_series_with_target(
    name: &str,
    values: &[Value],
    target: JsonTarget,
) -> Series {
    match target {
        JsonTarget::Utf8 => {
            let mut buf: Vec<Option<String>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Null => buf.push(None),
                    Value::Bool(b) => buf.push(Some(b.to_string())),
                    Value::Number(n) => buf.push(Some(n.to_string())),
                    Value::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(Some(s.clone()))
                        }
                    }
                    Value::Array(_) | Value::Object(_) => buf.push(Some(v.to_string())),
                }
            }
            Series::new(name.into(), buf)
        }
        JsonTarget::Boolean => {
            let mut buf: Vec<Option<bool>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Bool(b) => buf.push(Some(*b)),
                    Value::String(s) => buf.push(as_bool_like(s)),
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        JsonTarget::Float64 => {
            let mut buf: Vec<Option<f64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            buf.push(Some(f))
                        } else if let Some(i) = n.as_i64() {
                            buf.push(Some(i as f64))
                        } else if let Some(u) = n.as_u64() {
                            buf.push(Some(u as f64))
                        } else {
                            buf.push(None)
                        }
                    }
                    Value::Null => buf.push(None),
                    Value::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        JsonTarget::Int64 => {
            let mut buf: Vec<Option<i64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Number(n) => buf.push(n.as_i64()),
                    Value::Null => buf.push(None),
                    Value::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        JsonTarget::UInt64 => {
            let mut buf: Vec<Option<u64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Value::Number(n) => buf.push(n.as_u64()),
                    Value::Null => buf.push(None),
                    Value::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
    }
}

pub fn calamine_value_to_series(name: &str, values: &[Data]) -> Series {
    // 与 serde_values_to_series 相同的目标类型判定
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Target {
        Utf8,
        Boolean,
        Float64,
        Int64,
        UInt64,
    }

    let mut has_non_bool_string = false;
    let mut has_bool = false;
    let mut has_number = false;
    let mut has_float = false;
    let mut has_i64 = false;
    let mut has_u64 = false;

    for v in values.iter() {
        match v {
            Data::Empty => {}
            Data::Error(_) => {}
            Data::Bool(_) => {
                if has_number {
                    has_non_bool_string = true;
                    break;
                }
                has_bool = true;
            }
            Data::Int(_) => {
                if has_bool {
                    has_non_bool_string = true;
                    break;
                }
                has_number = true;
                has_i64 = true;
            }
            Data::Float(_) | Data::DateTime(_) | Data::DateTimeIso(_) | Data::DurationIso(_) => {
                if has_bool {
                    has_non_bool_string = true;
                    break;
                }
                has_number = true;
                has_float = true;
            }
            Data::String(s) => {
                let bool_like = as_bool_like(s);
                let null_like = is_null_like(s);
                if bool_like.is_some() || null_like {
                    if bool_like.is_some() {
                        has_bool = true;
                    }
                } else {
                    has_non_bool_string = true;
                    break;
                }
            }
        }
    }

    let target = if has_non_bool_string || (has_number && has_bool) {
        Target::Utf8
    } else if has_number {
        if has_float || (has_i64 && has_u64) {
            Target::Float64
        } else if has_i64 {
            Target::Int64
        } else {
            Target::UInt64
        }
    } else if has_bool {
        Target::Boolean
    } else {
        Target::Utf8
    };

    match target {
        Target::Utf8 => {
            let mut buf: Vec<Option<String>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Data::Empty => buf.push(None),
                    Data::Error(_) => buf.push(None),
                    Data::Bool(b) => buf.push(Some(b.to_string())),
                    Data::Int(i) => buf.push(Some(i.to_string())),
                    Data::Float(f) => buf.push(Some(f.to_string())),
                    Data::DateTime(dt) => buf.push(Some(dt.as_f64().to_string())),
                    Data::DateTimeIso(dt) => buf.push(Some(dt.to_string())),
                    Data::DurationIso(d) => buf.push(Some(d.to_string())),
                    Data::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(Some(s.clone()))
                        }
                    }
                }
            }
            Series::new(name.into(), buf)
        }
        Target::Boolean => {
            let mut buf: Vec<Option<bool>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Data::Bool(b) => buf.push(Some(*b)),
                    Data::String(s) => buf.push(as_bool_like(s)),
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        Target::Float64 => {
            let mut buf: Vec<Option<f64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Data::Int(i) => buf.push(Some(*i as f64)),
                    Data::Float(f) => buf.push(Some(*f)),
                    Data::DateTime(dt) => {
                        // ExcelDateTime 可以转换为 f64 (Excel 序列号)
                        buf.push(Some(dt.as_f64()))
                    }
                    Data::DateTimeIso(_) => buf.push(None), // 无法直接转换为 f64
                    Data::DurationIso(_) => buf.push(None), // 无法直接转换为 f64
                    Data::Empty => buf.push(None),
                    Data::Error(_) => buf.push(None),
                    Data::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    Data::Bool(_) => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        Target::Int64 => {
            let mut buf: Vec<Option<i64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Data::Int(i) => buf.push(Some(*i)),
                    Data::Empty => buf.push(None),
                    Data::Error(_) => buf.push(None),
                    Data::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
        Target::UInt64 => {
            let mut buf: Vec<Option<u64>> = Vec::with_capacity(values.len());
            for v in values.iter() {
                match v {
                    Data::Int(i) if *i >= 0 => buf.push(Some(*i as u64)),
                    Data::Empty => buf.push(None),
                    Data::Error(_) => buf.push(None),
                    Data::String(s) => {
                        if is_null_like(s) {
                            buf.push(None)
                        } else {
                            buf.push(None)
                        }
                    }
                    _ => buf.push(None),
                }
            }
            Series::new(name.into(), buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::type_convert::{calamine_value_to_series, json_value_to_any_value};
    use calamine::Data;
    use serde_json::Value;

    #[test]
    fn test_json_value_to_any_value() {
        let s = vec![
            Value::from(1),
            Value::from(2),
            Value::from(3),
            Value::from(4.0),
        ];
        let a = json_value_to_any_value(&s);
        println!("{:?}", a);
    }

    #[test]
    fn test_calamine_value_to_series() {
        let values = vec![
            Data::Int(1),
            Data::Int(2),
            Data::Float(3.5),
            Data::String("test".to_string()),
            Data::Bool(true),
            Data::Empty,
        ];

        let series = calamine_value_to_series("test_column", &values);
        println!("Series: {:?}", series);
        assert_eq!(series.name(), "test_column");
    }

    #[test]
    fn test_calamine_bool_series() {
        let values = vec![
            Data::Bool(true),
            Data::Bool(false),
            Data::String("true".to_string()),
            Data::String("false".to_string()),
            Data::Empty,
        ];

        let series = calamine_value_to_series("bool_column", &values);
        println!("Bool Series: {:?}", series);
        assert_eq!(series.dtype(), &polars::prelude::DataType::Boolean);
    }

    #[test]
    fn test_calamine_int_series() {
        let values = vec![Data::Int(10), Data::Int(20), Data::Int(30), Data::Empty];

        let series = calamine_value_to_series("int_column", &values);
        println!("Int Series: {:?}", series);
        assert_eq!(series.dtype(), &polars::prelude::DataType::Int64);
    }
}
