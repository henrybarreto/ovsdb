use crate::model::{AtomicType, BaseType, DatabaseSchema, MaxSize, TableSchema, Type};
use serde_json::{json, Value};

/// Builders and validators for OVSDB transaction operations.
#[derive(Debug, Clone, Copy, Default)]
pub struct Ops;

impl Ops {
    /// Validate a datum against an OVSDB type.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the datum shape or contents do not match the type.
    pub fn validate_datum(typ: &Type, val: &Value) -> Result<(), String> {
        match typ {
            Type::Atomic(base) => base.validate(val),
            Type::Complex {
                key,
                value: None,
                min,
                max,
            } => Self::validate_set_datum(key, *min, max, val),
            Type::Complex {
                key,
                value: Some(map_value),
                min,
                max,
            } => Self::validate_map_datum(key, map_value, *min, max, val),
        }
    }

    fn validate_set_datum(
        key: &BaseType,
        min: i64,
        max: &MaxSize,
        val: &Value,
    ) -> Result<(), String> {
        match val.as_array() {
            Some(arr) if arr.first() == Some(&json!("set")) => {
                let inner = arr
                    .get(1)
                    .and_then(Value::as_array)
                    .ok_or("set must have array")?;
                let count = i64::try_from(inner.len()).map_err(|_| "set too large")?;
                Self::ensure_minimum_size(count, min)?;
                Self::ensure_maximum_size(count, max)?;
                inner.iter().try_for_each(|item| key.validate(item))?;
                Ok(())
            }
            Some(arr) if matches!(arr.first(), Some(v) if v == "set" || v == "map") => {
                Err("tagged set/map values MUST have two elements".into())
            }
            _ => key.validate(val),
        }
    }

    fn validate_map_datum(
        key: &BaseType,
        map_value: &BaseType,
        min: i64,
        max: &MaxSize,
        val: &Value,
    ) -> Result<(), String> {
        match val.as_array() {
            Some(arr) if arr.first() == Some(&json!("map")) => {
                let inner = arr
                    .get(1)
                    .and_then(Value::as_array)
                    .ok_or("map must have array")?;
                let count = i64::try_from(inner.len()).map_err(|_| "map too large")?;
                Self::ensure_minimum_size(count, min)?;
                Self::ensure_maximum_size(count, max)?;
                inner
                    .iter()
                    .try_for_each(|pair| Self::validate_map_pair(key, map_value, pair))?;
                Ok(())
            }
            Some(arr) if matches!(arr.first(), Some(v) if v == "set" || v == "map") => {
                Err("tagged set/map values MUST have two elements".into())
            }
            _ => Err("map values MUST be encoded as [\"map\", [...]]".into()),
        }
    }

    /// Validate a transaction against a database schema.
    ///
    /// # Errors
    ///
    /// Returns `Err` when an operation references an unknown table, column,
    /// or uses an invalid datum or mutation shape.
    pub fn validate_transaction(schema: &DatabaseSchema, ops: &[Value]) -> Result<(), String> {
        for op in ops {
            let obj = op.as_object().ok_or("op not object")?;
            let table_name = obj
                .get("table")
                .and_then(Value::as_str)
                .ok_or("table missing")?;
            let table = schema.tables.get(table_name).ok_or("unknown table")?;
            let op_name = obj.get("op").and_then(Value::as_str).ok_or("op missing")?;

            match op_name {
                "insert" | "update" => Self::validate_row_operation(op_name, table, obj)?,
                "mutate" => Self::validate_mutations(table, obj)?,
                _ => {}
            }
        }
        Ok(())
    }

    fn ensure_minimum_size(count: i64, min: i64) -> Result<(), String> {
        if count < min {
            Err("too few elements".into())
        } else {
            Ok(())
        }
    }

    fn ensure_maximum_size(count: i64, max: &MaxSize) -> Result<(), String> {
        if let MaxSize::Integer(m) = max {
            if count > *m {
                return Err("too many elements".into());
            }
        }
        Ok(())
    }

    fn validate_map_pair(key: &BaseType, map_value: &BaseType, pair: &Value) -> Result<(), String> {
        let p = pair.as_array().ok_or("map pair must be array")?;
        if p.len() != 2 {
            return Err("map pair must contain exactly two elements".into());
        }
        key.validate(
            p.first()
                .ok_or("map pair must contain exactly two elements")?,
        )?;
        map_value.validate(
            p.get(1)
                .ok_or("map pair must contain exactly two elements")?,
        )?;
        Ok(())
    }

    fn validate_row_operation(
        op_name: &str,
        table: &TableSchema,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<(), String> {
        let row = obj
            .get("row")
            .and_then(Value::as_object)
            .ok_or("row missing")?;
        row.iter()
            .try_for_each(|(col_name, val)| Self::validate_row_field(op_name, table, col_name, val))
    }

    fn validate_row_field(
        op_name: &str,
        table: &TableSchema,
        col_name: &str,
        val: &Value,
    ) -> Result<(), String> {
        let col = table.columns.get(col_name).ok_or("unknown column")?;
        if op_name == "update" && col.mutable == Some(false) {
            return Err(format!("column {col_name} is immutable"));
        }
        Self::validate_datum(&col.r#type, val)
    }

    fn validate_mutations(
        table: &TableSchema,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<(), String> {
        let mutations = obj
            .get("mutations")
            .and_then(Value::as_array)
            .ok_or("mutations missing")?;
        mutations
            .iter()
            .try_for_each(|mutation| Self::validate_mutation(table, mutation))
    }

    fn validate_mutation(table: &TableSchema, mutation: &Value) -> Result<(), String> {
        let arr = mutation.as_array().ok_or("mutation must be array")?;
        if arr.len() != 3 {
            return Err("mutation must have exactly three elements".into());
        }

        let column_name = arr
            .first()
            .and_then(Value::as_str)
            .ok_or("mutation column must be string")?;
        let mutator = arr
            .get(1)
            .and_then(Value::as_str)
            .ok_or("mutation mutator must be string")?;
        let value = arr.get(2).ok_or("mutation value must be present")?;
        let col = table.columns.get(column_name).ok_or("unknown column")?;

        if column_name == "_uuid" || column_name == "_version" {
            return Err("internal columns may not be mutated".into());
        }
        if col.mutable == Some(false) {
            return Err(format!("column {column_name} is immutable"));
        }

        match &col.r#type {
            Type::Atomic(base) => Self::validate_atomic_mutation(base, mutator, value),
            Type::Complex {
                key, value: None, ..
            } => Self::validate_set_mutation(key, mutator, value),
            Type::Complex {
                key,
                value: Some(map_value),
                ..
            } => Self::validate_map_mutation(key, map_value, mutator, value),
        }
    }

    fn validate_atomic_mutation(
        base: &BaseType,
        mutator: &str,
        value: &Value,
    ) -> Result<(), String> {
        base.validate_unconstrained(value)?;
        match base.atomic_type() {
            AtomicType::Integer => {
                if !matches!(mutator, "+=" | "-=" | "*=" | "/=" | "%=") {
                    return Err("invalid mutator for integer".into());
                }
            }
            AtomicType::Real => {
                if !matches!(mutator, "+=" | "-=" | "*=" | "/=") {
                    return Err("invalid mutator for real".into());
                }
            }
            AtomicType::Boolean | AtomicType::String | AtomicType::Uuid => {
                return Err("no valid mutators for this type".into());
            }
        }
        Ok(())
    }

    fn validate_set_mutation(key: &BaseType, mutator: &str, value: &Value) -> Result<(), String> {
        match mutator {
            "insert" | "delete" => {}
            "+=" | "-=" | "*=" | "/=" | "%=" => {
                if !matches!(key.atomic_type(), AtomicType::Integer | AtomicType::Real) {
                    return Err("numeric mutators only valid for numeric set elements".into());
                }
            }
            _ => return Err("invalid mutator for set".into()),
        }
        key.validate_unconstrained(value)?;
        Ok(())
    }

    fn validate_map_mutation(
        key: &BaseType,
        value_type: &BaseType,
        mutator: &str,
        value: &Value,
    ) -> Result<(), String> {
        match mutator {
            "insert" => {
                let inner = Self::map_entries(value, "map insert value must be a map")?;
                inner.iter().try_for_each(|pair| -> Result<(), String> {
                    let pair = pair.as_array().ok_or("map entry must be array")?;
                    if pair.len() != 2 {
                        return Err("map entry must contain two elements".into());
                    }
                    key.validate_unconstrained(
                        pair.first().ok_or("map entry must contain two elements")?,
                    )?;
                    value_type.validate_unconstrained(
                        pair.get(1).ok_or("map entry must contain two elements")?,
                    )?;
                    Ok(())
                })?;
            }
            "delete" => {
                let inner = value
                    .as_array()
                    .ok_or("map delete value must be map or set")?;
                if matches!(inner.first(), Some(v) if v == "set") {
                    let values = inner
                        .get(1)
                        .and_then(Value::as_array)
                        .ok_or("map delete set must be array")?;
                    values
                        .iter()
                        .try_for_each(|key_value| key.validate_unconstrained(key_value))?;
                    return Ok(());
                }

                let entries = Self::map_entries(value, "map delete value must be map or set")?;
                entries.iter().try_for_each(|pair| -> Result<(), String> {
                    let pair = pair
                        .as_array()
                        .ok_or("map delete value must be map or set")?;
                    if pair.len() != 2 {
                        return Err("map delete value must be map or set".into());
                    }
                    key.validate_unconstrained(
                        pair.first().ok_or("map delete value must be map or set")?,
                    )?;
                    value_type.validate_unconstrained(
                        pair.get(1).ok_or("map delete value must be map or set")?,
                    )?;
                    Ok(())
                })?;
            }
            _ => return Err("invalid mutator for map".into()),
        }
        Ok(())
    }

    fn map_entries<'a>(value: &'a Value, err: &str) -> Result<&'a [Value], String> {
        let inner = value.as_array().ok_or_else(|| err.to_string())?;
        if inner.first().is_some_and(|v| v == "map") {
            return inner
                .get(1)
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .ok_or_else(|| err.to_string());
        }
        Ok(inner.as_slice())
    }

    fn op_object(op: &str, fields: Vec<(&str, Value)>) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("op".to_string(), json!(op));
        for (k, v) in fields {
            map.insert(k.to_string(), v);
        }
        Value::Object(map)
    }

    /// Build an `update` transaction operation.
    pub fn update(table: &str, r#where: &[Value], row: Value) -> Value {
        Self::op_object(
            "update",
            vec![
                ("table", json!(table)),
                ("where", json!(r#where)),
                ("row", row),
            ],
        )
    }

    /// Build a `mutate` transaction operation.
    pub fn mutate(table: &str, r#where: &[Value], mutations: &[Value]) -> Value {
        Self::op_object(
            "mutate",
            vec![
                ("table", json!(table)),
                ("where", json!(r#where)),
                ("mutations", json!(mutations)),
            ],
        )
    }

    /// Build a `wait` transaction operation.
    pub fn wait(
        table: &str,
        r#where: &[Value],
        columns: &[String],
        until: &str,
        rows: &[Value],
        timeout: Option<i64>,
    ) -> Value {
        let mut f = vec![
            ("table", json!(table)),
            ("where", json!(r#where)),
            ("columns", json!(columns)),
            ("until", json!(until)),
            ("rows", json!(rows)),
        ];
        if let Some(t) = timeout {
            f.push(("timeout", json!(t)));
        }
        Self::op_object("wait", f)
    }

    /// Build a `commit` transaction operation.
    pub fn commit(durable: bool) -> Value {
        Self::op_object("commit", vec![("durable", json!(durable))])
    }

    /// Build an `abort` transaction operation.
    pub fn abort() -> Value {
        Self::op_object("abort", vec![])
    }

    /// Build a `comment` transaction operation.
    pub fn comment(comment: &str) -> Value {
        Self::op_object("comment", vec![("comment", json!(comment))])
    }

    /// Build an `assert` transaction operation.
    pub fn assert(lock: &str) -> Value {
        Self::op_object("assert", vec![("lock", json!(lock))])
    }

    /// Build a `delete` transaction operation.
    pub fn delete(table: &str, r#where: &[Value]) -> Value {
        Self::op_object(
            "delete",
            vec![("table", json!(table)), ("where", json!(r#where))],
        )
    }

    /// Build an `insert` transaction operation.
    pub fn insert(table: &str, row: Value, uuid_name: Option<&str>) -> Value {
        let mut fields = vec![("table", json!(table)), ("row", row)];
        if let Some(uuid_name) = uuid_name {
            fields.push(("uuid-name", json!(uuid_name)));
        }
        Self::op_object("insert", fields)
    }

    /// Build a `select` transaction operation.
    pub fn select(table: &str, r#where: &[Value], columns: Option<&[String]>) -> Value {
        let mut f = vec![("table", json!(table)), ("where", json!(r#where))];
        if let Some(c) = columns {
            f.push(("columns", json!(c)));
        }
        Self::op_object("select", f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AtomicType, BaseType, MaxSize, Type};
    use serde_json::json;

    #[test]
    fn test_op_builders_emit_expected_shapes() {
        let where_clause = vec![json!(["name", "==", "foo"])];
        let columns = vec!["name".to_string(), "external_ids".to_string()];
        let mutations = vec![json!(["external_ids", "insert", ["map", [["k", "v"]]]])];

        assert_eq!(
            Ops::update("MyTable", &where_clause, json!({"name": "bar"})),
            json!({
                "op": "update",
                "table": "MyTable",
                "where": [["name", "==", "foo"]],
                "row": {"name": "bar"}
            })
        );
        assert_eq!(
            Ops::mutate("MyTable", &where_clause, &mutations),
            json!({
                "op": "mutate",
                "table": "MyTable",
                "where": [["name", "==", "foo"]],
                "mutations": [["external_ids", "insert", ["map", [["k", "v"]]]]]
            })
        );
        assert_eq!(
            Ops::wait(
                "MyTable",
                &where_clause,
                &columns,
                "==",
                &[json!({"name": "bar"})],
                Some(5)
            ),
            json!({
                "op": "wait",
                "table": "MyTable",
                "where": [["name", "==", "foo"]],
                "columns": ["name", "external_ids"],
                "until": "==",
                "rows": [{"name": "bar"}],
                "timeout": 5
            })
        );
        assert_eq!(Ops::commit(true), json!({"op": "commit", "durable": true}));
        assert_eq!(Ops::abort(), json!({"op": "abort"}));
        assert_eq!(
            Ops::comment("note"),
            json!({"op": "comment", "comment": "note"})
        );
        assert_eq!(Ops::assert("lock"), json!({"op": "assert", "lock": "lock"}));
        assert_eq!(
            Ops::delete("MyTable", &where_clause),
            json!({
                "op": "delete",
                "table": "MyTable",
                "where": [["name", "==", "foo"]]
            })
        );
        assert_eq!(
            Ops::insert("MyTable", json!({"name": "bar"}), Some("row1")),
            json!({
                "op": "insert",
                "table": "MyTable",
                "row": {"name": "bar"},
                "uuid-name": "row1"
            })
        );
        assert_eq!(
            Ops::select("MyTable", &where_clause, Some(&columns)),
            json!({
                "op": "select",
                "table": "MyTable",
                "where": [["name", "==", "foo"]],
                "columns": ["name", "external_ids"]
            })
        );
    }

    #[test]
    fn test_validate_datum_rejects_wrong_set_and_map_shapes() {
        let set_type = Type::Complex {
            key: BaseType::Atomic(AtomicType::String),
            value: None,
            min: 0,
            max: MaxSize::Unlimited("unlimited".to_string()),
        };
        assert!(Ops::validate_datum(&set_type, &json!(["set", 1])).is_err());
        assert!(Ops::validate_datum(&set_type, &json!(["set", ["a", "b"]])).is_ok());

        let map_type = Type::Complex {
            key: BaseType::Atomic(AtomicType::String),
            value: Some(BaseType::Atomic(AtomicType::String)),
            min: 0,
            max: MaxSize::Unlimited("unlimited".to_string()),
        };
        assert!(Ops::validate_datum(&map_type, &json!(["map", [["a"]]])).is_err());
        assert!(Ops::validate_datum(&map_type, &json!(["map", [["a", "b"]]])).is_ok());
    }
}
