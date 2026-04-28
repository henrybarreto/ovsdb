use crate::strings::reject_null_bytes;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::TryFrom;

/// An OVSDB error object preserved from the server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcError {
    /// The server error name.
    pub error: String,
    /// Optional human-readable details from the server.
    pub details: Option<String>,
    /// Any additional fields returned by the server.
    #[serde(flatten)]
    pub other: serde_json::Map<String, Value>,
}

/// A datum value used by OVSDB schemas and rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Datum {
    /// Integer datum.
    Integer(i64),
    /// Floating-point datum.
    Real(f64),
    /// Boolean datum.
    Boolean(bool),
    /// String datum.
    String(String),
    /// UUID datum.
    Uuid(String),
    /// Named UUID datum.
    NamedUuid(String),
    /// Set datum.
    Set(Vec<Self>),
    /// Map datum.
    Map(Vec<(Self, Self)>),
}

/// A database schema description returned by OVSDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSchema {
    /// Database name.
    pub name: String,
    /// Schema version string.
    pub version: String,
    /// Optional checksum supplied by the server.
    pub cksum: Option<String>,
    /// Tables defined by the schema.
    pub tables: HashMap<String, TableSchema>,
}

/// A single OVSDB table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    /// Table columns keyed by name.
    pub columns: HashMap<String, ColumnSchema>,
    /// Optional maximum row count.
    pub max_rows: Option<i64>,
    /// Whether this table is a root table.
    pub is_root: Option<bool>,
    /// Optional table indexes.
    pub indexes: Option<Vec<Vec<String>>>,
}

/// A single OVSDB column schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// The column type.
    pub r#type: Type,
    /// Whether the column is ephemeral.
    pub ephemeral: Option<bool>,
    /// Whether the column is mutable.
    pub mutable: Option<bool>,
}

/// OVSDB type declaration for a column or value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    /// Atomic value type.
    Atomic(BaseType),
    /// Complex set or map type.
    Complex {
        /// The set or map key type.
        key: BaseType,
        /// The map value type, if this is a map.
        value: Option<BaseType>,
        /// Minimum allowed cardinality.
        #[serde(default = "default_min")]
        min: i64,
        /// Maximum allowed cardinality.
        #[serde(default = "default_max")]
        max: MaxSize,
    },
}

const fn default_min() -> i64 {
    1
}
const fn default_max() -> MaxSize {
    MaxSize::Integer(1)
}

/// Atomic OVSDB value types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AtomicType {
    /// Integer values.
    Integer,
    /// Real values.
    Real,
    /// Boolean values.
    Boolean,
    /// String values.
    String,
    /// UUID values.
    Uuid,
}

/// Maximum size bound for set and map types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MaxSize {
    /// A finite maximum size.
    Integer(i64),
    /// An unlimited maximum size.
    Unlimited(String), // "unlimited"
}

/// Base OVSDB type configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BaseType {
    /// A plain atomic type.
    Atomic(AtomicType),
    /// An atomic type with constraints.
    Configured {
        /// The underlying atomic type.
        #[serde(rename = "type")]
        r#type: AtomicType,
        /// Optional enum constraint encoded as an OVSDB set.
        r#enum: Option<Value>,
        /// Optional minimum integer bound.
        #[serde(rename = "minInteger")]
        min_integer: Option<i64>,
        /// Optional maximum integer bound.
        #[serde(rename = "maxInteger")]
        max_integer: Option<i64>,
        /// Optional minimum real bound.
        #[serde(rename = "minReal")]
        min_real: Option<f64>,
        /// Optional maximum real bound.
        #[serde(rename = "maxReal")]
        max_real: Option<f64>,
        /// Optional minimum string length.
        #[serde(rename = "minLength")]
        min_length: Option<i64>,
        /// Optional maximum string length.
        #[serde(rename = "maxLength")]
        max_length: Option<i64>,
        /// Optional referenced table name for UUID columns.
        #[serde(rename = "refTable")]
        ref_table: Option<String>,
        /// Optional reference strength.
        #[serde(rename = "refType")]
        ref_type: Option<RefType>,
    },
}

/// Reference strength for UUID columns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RefType {
    /// Strong reference.
    Strong,
    /// Weak reference.
    Weak,
}

impl Type {
    /// Return the key/base type for this OVSDB type.
    pub const fn key(&self) -> &BaseType {
        match self {
            Self::Atomic(a) => a,
            Self::Complex { key, .. } => key,
        }
    }

    /// Validate the structural shape of the type declaration.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the type declaration uses invalid bounds or
    /// incompatible enum/ref settings.
    pub fn validate_shape(&self) -> Result<(), String> {
        match self {
            Self::Atomic(base) => base.validate_shape(),
            Self::Complex {
                key,
                value,
                min,
                max,
            } => {
                key.validate_shape()?;
                if let Some(value) = value {
                    value.validate_shape()?;
                }
                if *min != 0 && *min != 1 {
                    return Err(format!("invalid type min {min}"));
                }
                match max {
                    MaxSize::Integer(m) if *m < *min => {
                        Err(format!("type max {m} must be >= min {min}"))
                    }
                    MaxSize::Integer(m) if *m < 1 => Err(format!("invalid type max {m}")),
                    MaxSize::Integer(_) | MaxSize::Unlimited(_) => Ok(()),
                }
            }
        }
    }
}

impl BaseType {
    /// Validate the structural shape of the base type declaration.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the base type mixes incompatible constraints or
    /// uses invalid enum encoding.
    pub fn validate_shape(&self) -> Result<(), String> {
        match self {
            Self::Atomic(_) => Ok(()),
            Self::Configured {
                r#type,
                r#enum,
                min_integer,
                max_integer,
                min_real,
                max_real,
                min_length,
                max_length,
                ref_table,
                ..
            } => {
                if let Some(enum_value) = r#enum {
                    if min_integer.is_some()
                        || max_integer.is_some()
                        || min_real.is_some()
                        || max_real.is_some()
                        || min_length.is_some()
                        || max_length.is_some()
                        || ref_table.is_some()
                    {
                        return Err(
                            "enum is mutually exclusive with range and ref constraints".into()
                        );
                    }
                    let enum_items = enum_value
                        .as_array()
                        .ok_or_else(|| "enum must be a set".to_string())?;
                    if enum_items.len() != 2 || enum_items.first() != Some(&json!("set")) {
                        return Err("enum must be encoded as [\"set\", [...]]".into());
                    }
                    let values = enum_items
                        .get(1)
                        .and_then(Value::as_array)
                        .ok_or_else(|| "enum set must contain an array".to_string())?;
                    if values.is_empty() {
                        return Err("enum must contain at least one value".into());
                    }
                    if values
                        .iter()
                        .any(|value| !Self::matches_atomic_type(r#type, value))
                    {
                        return Err("enum values must match the base atomic type".into());
                    }
                }

                if matches!(r#type, AtomicType::Integer)
                    && (min_real.is_some()
                        || max_real.is_some()
                        || min_length.is_some()
                        || max_length.is_some())
                {
                    return Err("integer base type cannot use real or length bounds".into());
                }
                if matches!(r#type, AtomicType::Real)
                    && (min_integer.is_some()
                        || max_integer.is_some()
                        || min_length.is_some()
                        || max_length.is_some())
                {
                    return Err("real base type cannot use integer or length bounds".into());
                }
                if matches!(r#type, AtomicType::String)
                    && (min_integer.is_some()
                        || max_integer.is_some()
                        || min_real.is_some()
                        || max_real.is_some())
                {
                    return Err("string base type cannot use integer or real bounds".into());
                }
                if matches!(r#type, AtomicType::Uuid)
                    && (min_integer.is_some()
                        || max_integer.is_some()
                        || min_real.is_some()
                        || max_real.is_some()
                        || min_length.is_some()
                        || max_length.is_some())
                {
                    return Err("uuid base type cannot use range or length bounds".into());
                }
                if let (Some(min), Some(max)) = (min_integer, max_integer) {
                    if max < min {
                        return Err("integer maxInteger must be >= minInteger".into());
                    }
                }
                if let (Some(min), Some(max)) = (min_real, max_real) {
                    if max < min {
                        return Err("real maxReal must be >= minReal".into());
                    }
                }
                if let (Some(min), Some(max)) = (min_length, max_length) {
                    if max < min {
                        return Err("string maxLength must be >= minLength".into());
                    }
                }

                Ok(())
            }
        }
    }

    /// Return the underlying atomic type.
    pub const fn atomic_type(&self) -> &AtomicType {
        match self {
            Self::Atomic(a) => a,
            Self::Configured { r#type, .. } => r#type,
        }
    }

    /// Validate a value against this base type, including constraints.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the value does not match the type, constraints, or
    /// enum definition.
    pub fn validate(&self, val: &Value) -> Result<(), String> {
        let typ = self.atomic_type();
        match typ {
            AtomicType::Integer => self.validate_integer(val)?,
            AtomicType::Real => self.validate_real(val)?,
            AtomicType::Boolean => {
                if !val.is_boolean() {
                    return Err("not a boolean".to_string());
                }
            }
            AtomicType::String => self.validate_string(val)?,
            AtomicType::Uuid => {
                let is_uuid = Self::is_uuid_array(val);
                if !is_uuid {
                    return Err("not a uuid".to_string());
                }
            }
        }

        self.validate_enum(val)
    }

    /// Validate a value against this base type without constraint checks.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the value is not of the expected atomic shape.
    pub fn validate_unconstrained(&self, val: &Value) -> Result<(), String> {
        match self.atomic_type() {
            AtomicType::Integer => {
                if !val.is_i64() {
                    return Err("not an integer".to_string());
                }
            }
            AtomicType::Real => {
                if !val.is_number() {
                    return Err("not a real".to_string());
                }
            }
            AtomicType::Boolean => {
                if !val.is_boolean() {
                    return Err("not a boolean".to_string());
                }
            }
            AtomicType::String => {
                if !val.is_string() {
                    return Err("not a string".to_string());
                }
            }
            AtomicType::Uuid => {
                let is_uuid = Self::is_uuid_array(val);
                if !is_uuid {
                    return Err("not a uuid".to_string());
                }
            }
        }
        Ok(())
    }

    fn validate_integer(&self, val: &Value) -> Result<(), String> {
        let i = val.as_i64().ok_or_else(|| "not an integer".to_string())?;
        let Self::Configured {
            min_integer,
            max_integer,
            ..
        } = self
        else {
            return Ok(());
        };

        if let Some(min) = min_integer {
            if i < *min {
                return Err(format!("{i} < min {min}"));
            }
        }
        if let Some(max) = max_integer {
            if i > *max {
                return Err(format!("{i} > max {max}"));
            }
        }
        Ok(())
    }

    fn validate_real(&self, val: &Value) -> Result<(), String> {
        let f = val.as_f64().ok_or_else(|| "not a real".to_string())?;
        let Self::Configured {
            min_real, max_real, ..
        } = self
        else {
            return Ok(());
        };

        if let Some(min) = min_real {
            if f < *min {
                return Err(format!("{f} < min {min}"));
            }
        }
        if let Some(max) = max_real {
            if f > *max {
                return Err(format!("{f} > max {max}"));
            }
        }
        Ok(())
    }

    fn validate_string(&self, val: &Value) -> Result<(), String> {
        let s = val.as_str().ok_or_else(|| "not a string".to_string())?;
        let Self::Configured {
            min_length,
            max_length,
            ..
        } = self
        else {
            return Ok(());
        };

        let len = i64::try_from(s.chars().count())
            .map_err(|_| "string length overflowed i64".to_string())?;
        if let Some(min) = min_length {
            if len < *min {
                return Err(format!("length {len} < min {min}"));
            }
        }
        if let Some(max) = max_length {
            if len > *max {
                return Err(format!("length {len} > max {max}"));
            }
        }
        Ok(())
    }

    fn validate_enum(&self, val: &Value) -> Result<(), String> {
        let Self::Configured {
            r#enum: Some(e), ..
        } = self
        else {
            return Ok(());
        };

        let set = e.as_array().ok_or("enum must be a set")?;
        if set.first() != Some(&json!("set")) {
            return Err("enum must be encoded as [\"set\", [...]]".into());
        }

        let options = set
            .get(1)
            .and_then(Value::as_array)
            .ok_or("enum set inner must be array")?;
        if !options.contains(val) {
            return Err(format!("value {val} not in enum {options:?}"));
        }
        Ok(())
    }
}

impl DatabaseSchema {
    /// Validate the schema definition for the database as a whole.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the database name, version, table names, or table
    /// definitions are invalid.
    pub fn validate(&self) -> Result<(), String> {
        if !Self::is_id(&self.name) {
            return Err(format!("invalid database name {}", self.name));
        }
        if !Self::is_version(&self.version) {
            return Err(format!("invalid schema version {}", self.version));
        }
        for (table_name, table) in &self.tables {
            if !Self::is_id(table_name) {
                return Err(format!("invalid table name {table_name}"));
            }
            table.validate(self, table_name)?;
        }
        Ok(())
    }

    fn is_id(value: &str) -> bool {
        let mut chars = value.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !(first.is_ascii_alphabetic() || first == '_') {
            return false;
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    fn is_version(value: &str) -> bool {
        let mut parts = value.split('.');
        let ok_part = |part: Option<&str>| matches!(part, Some(p) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()));
        ok_part(parts.next())
            && ok_part(parts.next())
            && ok_part(parts.next())
            && parts.next().is_none()
    }
}

impl TableSchema {
    /// Validate a table schema against the enclosing database schema.
    ///
    /// # Errors
    ///
    /// Returns `Err` when row limits, column names, or indexes are invalid.
    pub fn validate(&self, schema: &DatabaseSchema, table_name: &str) -> Result<(), String> {
        if let Some(max_rows) = self.max_rows {
            if max_rows < 1 {
                return Err(format!("table {table_name} maxRows must be positive"));
            }
        }

        for (column_name, column) in &self.columns {
            if !DatabaseSchema::is_id(column_name) {
                return Err(format!("invalid column name {column_name}"));
            }
            column.validate(schema, table_name, column_name)?;
        }

        if let Some(indexes) = &self.indexes {
            for index in indexes {
                Self::validate_index(index, &self.columns, table_name)?;
            }
        }

        Ok(())
    }

    fn validate_index(
        index: &[String],
        columns: &std::collections::HashMap<String, ColumnSchema>,
        table_name: &str,
    ) -> Result<(), String> {
        if index.is_empty() {
            return Err(format!("table {table_name} has an empty index"));
        }
        let mut seen = std::collections::HashSet::new();
        for column_name in index {
            Self::validate_index_column(table_name, columns, &mut seen, column_name)?;
        }
        Ok(())
    }

    fn validate_index_column<'a>(
        table_name: &str,
        columns: &std::collections::HashMap<String, ColumnSchema>,
        seen: &mut std::collections::HashSet<&'a str>,
        column_name: &'a str,
    ) -> Result<(), String> {
        if !seen.insert(column_name) {
            return Err(format!(
                "table {table_name} index contains duplicate column {column_name}"
            ));
        }
        let column = columns.get(column_name).ok_or_else(|| {
            format!("table {table_name} index references unknown column {column_name}")
        })?;
        if column.ephemeral == Some(true) {
            return Err(format!(
                "table {table_name} index references ephemeral column {column_name}"
            ));
        }
        Ok(())
    }
}

impl ColumnSchema {
    /// Validate a column schema against the enclosing database schema.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the type definition or referenced schema elements
    /// are invalid.
    pub fn validate(
        &self,
        schema: &DatabaseSchema,
        table_name: &str,
        column_name: &str,
    ) -> Result<(), String> {
        self.r#type.validate_shape()?;

        match &self.r#type {
            Type::Atomic(base) => base.validate_definition(schema, table_name, column_name)?,
            Type::Complex { key, value, .. } => {
                key.validate_definition(schema, table_name, column_name)?;
                if let Some(value) = value {
                    value.validate_definition(schema, table_name, column_name)?;
                }
            }
        }

        Ok(())
    }
}

impl BaseType {
    /// Validate base-type schema references against the enclosing schema.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the base type uses an unknown reference table or a
    /// non-UUID reference type.
    pub fn validate_definition(
        &self,
        schema: &DatabaseSchema,
        table_name: &str,
        column_name: &str,
    ) -> Result<(), String> {
        self.validate_shape()?;
        if let Self::Configured {
            ref_table: Some(ref_table),
            r#type,
            ..
        } = self
        {
            if !schema.tables.contains_key(ref_table) {
                return Err(format!(
                    "column {table_name}.{column_name} references unknown table {ref_table}"
                ));
            }
            if !matches!(r#type, AtomicType::Uuid) {
                return Err(format!(
                    "column {table_name}.{column_name} refTable requires uuid type"
                ));
            }
        }
        Ok(())
    }

    fn matches_atomic_type(typ: &AtomicType, value: &Value) -> bool {
        match typ {
            AtomicType::Integer => value.as_i64().is_some(),
            AtomicType::Real => value.is_number(),
            AtomicType::Boolean => value.is_boolean(),
            AtomicType::String => value.is_string(),
            AtomicType::Uuid => Self::is_uuid_array(value),
        }
    }

    fn is_uuid_array(value: &Value) -> bool {
        matches!(
            value.as_array(),
            Some(arr) if matches!(arr.first(), Some(Value::String(tag)) if tag == "uuid")
                && matches!(arr.get(1), Some(Value::String(_)))
        )
    }
}

impl TryFrom<Value> for Datum {
    type Error = String;

    fn try_from(val: Value) -> Result<Self, Self::Error> {
        match val {
            Value::Number(n) => n.as_i64().map_or_else(
                || {
                    n.as_f64().map_or_else(
                        || Err("number is neither integer nor real".to_string()),
                        |f| Ok(Self::Real(f)),
                    )
                },
                |i| Ok(Self::Integer(i)),
            ),
            Value::Bool(b) => Ok(Self::Boolean(b)),
            Value::String(s) => {
                reject_null_bytes(&s).map_err(std::string::ToString::to_string)?;
                Ok(Self::String(s))
            }
            Value::Array(a) => Self::try_from_array(&a),
            other => Ok(Self::String(other.to_string())),
        }
    }
}

impl Datum {
    fn try_from_array(a: &[Value]) -> Result<Self, String> {
        if a.len() != 2 {
            return Err("array payload must be a tagged OVSDB value".to_string());
        }
        if a.first() == Some(&json!("uuid")) {
            let s = a
                .get(1)
                .and_then(Value::as_str)
                .ok_or_else(|| "uuid payload must be a string".to_string())?;
            return Ok(Self::Uuid(s.to_string()));
        }
        if a.first() == Some(&json!("named-uuid")) {
            let s = a
                .get(1)
                .and_then(Value::as_str)
                .ok_or_else(|| "named-uuid payload must be a string".to_string())?;
            return Ok(Self::NamedUuid(s.to_string()));
        }
        if a.first() == Some(&json!("set")) {
            let inner = a
                .get(1)
                .and_then(Value::as_array)
                .ok_or_else(|| "set payload must be an array".to_string())?;
            return Ok(Self::Set(
                inner
                    .iter()
                    .cloned()
                    .map(Self::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ));
        }
        if a.first() == Some(&json!("map")) {
            let inner = a
                .get(1)
                .and_then(Value::as_array)
                .ok_or_else(|| "map payload must be an array".to_string())?;
            return Self::try_from_map_array(inner);
        }
        Err("array payload must be a tagged OVSDB value".to_string())
    }

    fn try_from_map_array(inner: &[Value]) -> Result<Self, String> {
        let mut map_items = Vec::with_capacity(inner.len());
        for item in inner {
            let pair = item
                .as_array()
                .ok_or_else(|| "map entries must be arrays".to_string())?;
            if pair.len() != 2 {
                return Err("map entries must contain exactly two elements".to_string());
            }
            map_items.push((
                Self::try_from(
                    pair.first().cloned().ok_or_else(|| {
                        "map entries must contain exactly two elements".to_string()
                    })?,
                )?,
                Self::try_from(
                    pair.get(1).cloned().ok_or_else(|| {
                        "map entries must contain exactly two elements".to_string()
                    })?,
                )?,
            ));
        }
        Ok(Self::Map(map_items))
    }
}

impl From<Datum> for Value {
    fn from(datum: Datum) -> Self {
        match datum {
            Datum::Integer(i) => json!(i),
            Datum::Real(f) => json!(f),
            Datum::Boolean(b) => json!(b),
            Datum::String(s) => json!(s),
            Datum::Uuid(s) => json!(["uuid", s]),
            Datum::NamedUuid(s) => json!(["named-uuid", s]),
            Datum::Set(v) => {
                json!(["set", Self::Array(v.into_iter().map(Self::from).collect())])
            }
            Datum::Map(v) => {
                let arr = v
                    .into_iter()
                    .map(|(k, v)| Self::Array(vec![Self::from(k), Self::from(v)]))
                    .collect::<Vec<_>>();
                json!(["map", Self::Array(arr)])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_base_type_validation() {
        // Integer range
        let int_type = BaseType::Configured {
            r#type: AtomicType::Integer,
            r#enum: None,
            min_integer: Some(0),
            max_integer: Some(10),
            min_real: None,
            max_real: None,
            min_length: None,
            max_length: None,
            ref_table: None,
            ref_type: None,
        };
        assert!(int_type.validate(&json!(5)).is_ok());
        assert!(int_type.validate(&json!(-1)).is_err());
        assert!(int_type.validate(&json!(11)).is_err());

        // String length
        let str_type = BaseType::Configured {
            r#type: AtomicType::String,
            r#enum: None,
            min_integer: None,
            max_integer: None,
            min_real: None,
            max_real: None,
            min_length: Some(3),
            max_length: Some(5),
            ref_table: None,
            ref_type: None,
        };
        assert!(str_type.validate(&json!("abc")).is_ok());
        assert!(str_type.validate(&json!("ab")).is_err());
        assert!(str_type.validate(&json!("abcdef")).is_err());

        // Enum
        let enum_type = BaseType::Configured {
            r#type: AtomicType::String,
            r#enum: Some(json!(["set", ["apple", "banana"]])),
            min_integer: None,
            max_integer: None,
            min_real: None,
            max_real: None,
            min_length: None,
            max_length: None,
            ref_table: None,
            ref_type: None,
        };
        assert!(enum_type.validate(&json!("apple")).is_ok());
        assert!(enum_type.validate(&json!("orange")).is_err());
    }

    #[test]
    fn test_schema_deserialization_defaults() -> Result<(), serde_json::Error> {
        let schema_json = json!({
            "key": "string"
        });
        let ovs_type: Type = serde_json::from_value(schema_json)?;
        assert!(matches!(
            ovs_type,
            Type::Complex {
                min: 1,
                max: MaxSize::Integer(1),
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn test_datum_encoding() {
        let datum = Datum::Set(vec![Datum::Integer(1), Datum::Integer(2)]);
        let value: Value = datum.into();
        assert_eq!(value, json!(["set", [1, 2]]));

        let datum2 = Datum::Map(vec![(Datum::String("a".to_string()), Datum::Integer(1))]);
        let value2: Value = datum2.into();
        assert_eq!(value2, json!(["map", [["a", 1]]]));
    }

    #[test]
    fn test_schema_validation_rejects_ref_and_index_edge_cases() {
        let mut tables = HashMap::new();
        tables.insert(
            "Parent".to_string(),
            TableSchema {
                columns: HashMap::new(),
                max_rows: None,
                is_root: None,
                indexes: None,
            },
        );
        tables.insert(
            "Child".to_string(),
            TableSchema {
                columns: {
                    let mut columns = HashMap::new();
                    columns.insert(
                        "ref".to_string(),
                        ColumnSchema {
                            r#type: Type::Atomic(BaseType::Configured {
                                r#type: AtomicType::Uuid,
                                r#enum: None,
                                min_integer: None,
                                max_integer: None,
                                min_real: None,
                                max_real: None,
                                min_length: None,
                                max_length: None,
                                ref_table: Some("Missing".to_string()),
                                ref_type: Some(RefType::Strong),
                            }),
                            ephemeral: None,
                            mutable: None,
                        },
                    );
                    columns
                },
                max_rows: None,
                is_root: None,
                indexes: Some(vec![vec!["ref".to_string(), "ref".to_string()]]),
            },
        );
        let schema = DatabaseSchema {
            name: "good_name".into(),
            version: "1.0.0".into(),
            cksum: None,
            tables,
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn test_validate_shape_rejects_bad_type_bounds() {
        let bad_type = Type::Complex {
            key: BaseType::Atomic(AtomicType::String),
            value: None,
            min: 2,
            max: MaxSize::Integer(1),
        };
        assert!(bad_type.validate_shape().is_err());

        let bad_min = Type::Complex {
            key: BaseType::Atomic(AtomicType::String),
            value: None,
            min: 3,
            max: MaxSize::Unlimited("unlimited".to_string()),
        };
        assert!(bad_min.validate_shape().is_err());
    }

    #[test]
    fn test_validate_definition_rejects_missing_ref_table_and_wrong_type() {
        let mut tables = HashMap::new();
        tables.insert(
            "Parent".to_string(),
            TableSchema {
                columns: HashMap::new(),
                max_rows: None,
                is_root: None,
                indexes: None,
            },
        );
        let schema = DatabaseSchema {
            name: "good_name".into(),
            version: "1.0.0".into(),
            cksum: None,
            tables,
        };

        let missing_ref = BaseType::Configured {
            r#type: AtomicType::Uuid,
            r#enum: None,
            min_integer: None,
            max_integer: None,
            min_real: None,
            max_real: None,
            min_length: None,
            max_length: None,
            ref_table: Some("Missing".into()),
            ref_type: Some(RefType::Strong),
        };
        assert!(missing_ref
            .validate_definition(&schema, "Child", "ref")
            .is_err());

        let wrong_type = BaseType::Configured {
            r#type: AtomicType::String,
            r#enum: None,
            min_integer: None,
            max_integer: None,
            min_real: None,
            max_real: None,
            min_length: None,
            max_length: None,
            ref_table: Some("Parent".into()),
            ref_type: Some(RefType::Strong),
        };
        assert!(wrong_type
            .validate_definition(&schema, "Child", "ref")
            .is_err());
    }

    #[test]
    fn test_datum_try_from_handles_scalar_values() {
        assert!(matches!(Datum::try_from(json!(1)), Ok(Datum::Integer(1))));
        assert!(matches!(
            Datum::try_from(json!(1.5)),
            Ok(Datum::Real(v)) if (v - 1.5).abs() < f64::EPSILON
        ));
        assert!(matches!(
            Datum::try_from(json!(true)),
            Ok(Datum::Boolean(true))
        ));
        assert!(matches!(
            Datum::try_from(json!("hello")),
            Ok(Datum::String(s)) if s == "hello"
        ));
    }

    #[test]
    fn test_datum_try_from_handles_tagged_values() {
        assert!(matches!(
            Datum::try_from(json!(["uuid", "550e8400-e29b-41d4-a716-446655440000"])),
            Ok(Datum::Uuid(s)) if s == "550e8400-e29b-41d4-a716-446655440000"
        ));
        assert!(matches!(
            Datum::try_from(json!(["named-uuid", "row1"])),
            Ok(Datum::NamedUuid(s)) if s == "row1"
        ));
        assert!(matches!(
            Datum::try_from(json!(["set", [1, "x"]])),
            Ok(Datum::Set(items)) if items.len() == 2
        ));
        assert!(matches!(
            Datum::try_from(json!(["map", [[1, "x"]]])),
            Ok(Datum::Map(items)) if items.len() == 1
        ));
    }

    #[test]
    fn test_datum_try_from_rejects_bad_shapes() {
        assert!(Datum::try_from(json!(["set", 1])).is_err());
        assert!(Datum::try_from(json!("bad\0value")).is_err());
    }
}
