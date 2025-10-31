use piccolo::{IntoValue, Value, table::NextValue};

#[derive(Debug, thiserror::Error)]
pub enum LuaSerdeError {
    #[error("cannot use value {0:?} as key in object")]
    InvalidKeyType(String),
    #[error("cannot serialize value {0:?}")]
    InvalidValueType(String),
    #[error("fuel exhausted during serialization")]
    FuelExhausted,
}

const DEFAULT_FUEL: i32 = 1_000_000;
const STRING_FUEL_COST: i32 = 4;
const ARRAY_FUEL_COST: i32 = 20;
const OBJECT_FUEL_COST: i32 = 40;

fn to_json_value_fueled<'gc>(
    value: Value<'gc>,
    fuel: i32,
) -> Result<(i32, serde_json::Value), LuaSerdeError> {
    if fuel <= 0 {
        return Err(LuaSerdeError::FuelExhausted);
    }

    match value {
        Value::Nil => Ok((1, serde_json::Value::Null)),
        Value::Boolean(bool) => Ok((1, serde_json::Value::Bool(bool))),
        Value::Integer(i64) => Ok((1, serde_json::Value::Number(serde_json::Number::from(i64)))),
        Value::Number(f64) => serde_json::Number::from_f64(f64)
            .map(|v| (2, serde_json::Value::Number(v)))
            .ok_or_else(|| LuaSerdeError::InvalidValueType(format!("<number> {}", f64))),
        Value::String(string) => Ok((
            string.len() as i32 * STRING_FUEL_COST,
            serde_json::Value::String(string.to_string()),
        )),
        Value::Table(table) => {
            let is_empty_table = matches!(table.next(Value::Nil), NextValue::NotFound);
            if is_empty_table {
                return Ok((ARRAY_FUEL_COST, serde_json::Value::Array(vec![])));
            }

            let mut is_array = true;
            let mut obj = serde_json::Map::<String, serde_json::Value>::new();

            let mut current_index = 1;
            for (key, val) in table.iter() {
                if matches!(key, Value::Integer(idx) if idx == current_index) {
                    current_index += 1;
                } else {
                    is_array = false;
                }

                let json_key = match key {
                    Value::String(s) => s.to_string(),
                    Value::Integer(i64) => i64.to_string(),
                    Value::Number(f64) => f64.to_string(),
                    Value::Boolean(b) => b.to_string(),
                    _ => {
                        return Err(LuaSerdeError::InvalidKeyType(format!("{}", key)));
                    }
                };

                let json_val = to_json_value(val)?;
                obj.insert(json_key, json_val);
            }

            if is_array {
                let mut arr = vec![];
                let mut index = 1;
                while let Some(value) = obj.remove(&index.to_string()) {
                    arr.push(value);
                    index += 1;
                }
                Ok((
                    ARRAY_FUEL_COST * arr.len() as i32,
                    serde_json::Value::Array(arr),
                ))
            } else {
                Ok((
                    OBJECT_FUEL_COST * obj.len() as i32,
                    serde_json::Value::Object(obj),
                ))
            }
        }
        // Value::UserData(user_data) => {
        //     to_json_value_fueled(user_data.into_value(ctx), OBJECT_FUEL_COST)
        // }
        _ => Err(LuaSerdeError::InvalidValueType(format!("{}", value))),
    }
}

/// Convert a [`piccolo::Value`] into a [`serde_json::Value`], with a feature to limit the size of
/// serialized types.
pub fn to_json_value<'gc>(value: Value<'gc>) -> Result<serde_json::Value, LuaSerdeError> {
    let (_fuel_used, json_value) = to_json_value_fueled(value, DEFAULT_FUEL)?;
    Ok(json_value)
}
