use super::error::Error;
use serde_json::{Map, Value};
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};

/// OVSDB RPC encoding and response validation helpers.
#[derive(Debug, Clone, Copy, Default)]
pub struct Rpc;

/// Validation helpers for RPC method parameters and transaction results.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Validator;

impl Rpc {
    /// Encode an RPC request object with the provided method, id, and params.
    pub fn encode<I, P>(method: &str, id: I, params: P) -> Value
    where
        I: Into<Value>,
        P: Into<Value>,
    {
        Value::Object(Map::from_iter([
            ("method".to_string(), Value::String(method.to_string())),
            ("id".to_string(), id.into()),
            ("params".to_string(), params.into()),
        ]))
    }

    /// Validate the request parameter shape for a named RPC method.
    ///
    /// # Errors
    ///
    /// Returns `Validation` when the params do not match the method shape.
    pub(crate) fn validate_method_params(method: &str, params: &Value) -> Result<(), Error> {
        Validator::validate_method_params(method, params)
    }

    /// Validate a sequence of transaction operations.
    ///
    /// # Errors
    ///
    /// Returns `Validation` when the operations are malformed.
    pub fn validate_transact_ops(ops: &[Value]) -> Result<(), Error> {
        Validator::validate_transact_ops(ops)
    }

    /// Validate a single transaction operation object.
    ///
    /// # Errors
    ///
    /// Returns `Validation` when the operation shape is invalid.
    pub fn validate_transact_op(op: &Value) -> Result<(), Error> {
        Validator::validate_transact_op(op)
    }

    pub(super) fn validate_table_updates(obj: &Map<String, Value>) -> Result<(), Error> {
        Validator::validate_table_updates(obj)
    }
}

impl Validator {
    /// Validate the request parameter shape for a named RPC method.
    pub fn validate_method_params(method: &str, params: &Value) -> Result<(), Error> {
        let arr = params
            .as_array()
            .ok_or_else(|| Error::Validation(format!("{method} params MUST be an array")))?;

        match method {
            "list_dbs" if !arr.is_empty() => {
                return Err(Error::Validation("list_dbs params MUST be empty".into()));
            }
            "get_schema" if arr.len() != 1 || !arr.first().is_some_and(Value::is_string) => {
                return Err(Error::Validation(
                    "get_schema params MUST be [<db-name>]".into(),
                ));
            }
            "transact" => {
                if arr.is_empty() || !arr.first().is_some_and(Value::is_string) {
                    return Err(Error::Validation(
                        "transact params MUST be [<db-name>, <operation>*]".into(),
                    ));
                }
                Self::validate_transact_ops(arr.get(1..).unwrap_or(&[]))?;
            }
            "monitor" | "monitor_cond" => {
                Self::validate_monitor_request(method, arr)?;
            }
            "monitor_cancel" if arr.len() != 1 => {
                return Err(Error::Validation(
                    "monitor_cancel params MUST be [<monitor-id>]".into(),
                ));
            }
            "lock" | "steal" | "unlock"
                if arr.len() != 1 || !arr.first().is_some_and(Value::is_string) =>
            {
                return Err(Error::Validation(format!("{method} params MUST be [<id>]")));
            }
            _ => {}
        }

        Ok(())
    }

    fn validate_monitor_request(method: &str, arr: &[Value]) -> Result<(), Error> {
        if arr.len() != 3 {
            return Err(Error::Validation(format!(
                "{method} params MUST be [<db-name>, <json-value>, <monitor-requests>]"
            )));
        }
        if !arr.first().is_some_and(Value::is_string) {
            return Err(Error::Validation(format!(
                "{method} database MUST be a string"
            )));
        }
        let monitor_requests = arr.get(2).and_then(Value::as_object).ok_or_else(|| {
            Error::Validation(format!("{method} monitor-requests MUST be an object"))
        })?;
        let mut seen_columns_by_table: HashMap<String, HashSet<String>> = HashMap::new();
        for (table_name, requests) in monitor_requests {
            Self::validate_monitor_table_name(table_name)?;
            let request_list = Self::monitor_request_list(requests)?;
            let require_columns_and_select = request_list.len() > 1;
            let table_seen = seen_columns_by_table.entry(table_name.clone()).or_default();
            let mut seen_columns = HashSet::new();
            for request in request_list {
                Self::validate_monitor_entry(request, method == "monitor_cond")?;
                Self::validate_monitor_request_entry(
                    table_name,
                    request,
                    require_columns_and_select,
                    &mut seen_columns,
                    table_seen,
                )?;
            }
        }
        Ok(())
    }

    fn validate_monitor_table_name(table_name: &str) -> Result<(), Error> {
        if Self::is_id(table_name) {
            Ok(())
        } else {
            Err(Error::Validation(format!(
                "invalid monitor table name {table_name}"
            )))
        }
    }

    fn monitor_request_list(requests: &Value) -> Result<&[Value], Error> {
        requests.as_array().map_or_else(
            || {
                Err(Error::Validation(
                    "monitor-requests table entry MUST be an array".into(),
                ))
            },
            |list| Ok(list.as_slice()),
        )
    }

    fn validate_monitor_request_entry(
        table_name: &str,
        request: &Value,
        require_columns_and_select: bool,
        seen_columns: &mut HashSet<String>,
        table_seen: &mut HashSet<String>,
    ) -> Result<(), Error> {
        let obj = request
            .as_object()
            .ok_or_else(|| Error::Validation("monitor request MUST be an object".into()))?;
        if require_columns_and_select
            && (!obj.contains_key("columns") || !obj.contains_key("select"))
        {
            return Err(Error::Validation(
                "monitor requests with multiple entries MUST specify both columns and select"
                    .into(),
            ));
        }
        if let Some(columns) = obj.get("columns").and_then(Value::as_array) {
            Self::validate_monitor_columns(table_name, columns, seen_columns)?;
            Self::validate_monitor_columns_overlap(table_name, columns, table_seen)?;
        }
        Ok(())
    }

    fn validate_monitor_columns(
        table_name: &str,
        columns: &[Value],
        seen_columns: &mut HashSet<String>,
    ) -> Result<(), Error> {
        for column in columns {
            let column = column
                .as_str()
                .ok_or_else(|| Error::Validation("monitor columns MUST contain strings".into()))?;
            if !seen_columns.insert(column.to_string()) {
                return Err(Error::Validation(format!(
                    "monitor table {table_name} contains duplicate monitored column {column}"
                )));
            }
        }
        Ok(())
    }

    fn validate_monitor_columns_overlap(
        table_name: &str,
        columns: &[Value],
        table_seen: &mut HashSet<String>,
    ) -> Result<(), Error> {
        for column in columns {
            let column = column
                .as_str()
                .ok_or_else(|| Error::Validation("monitor columns MUST contain strings".into()))?;
            if !table_seen.insert(column.to_string()) {
                return Err(Error::Validation(format!(
                    "monitor table {table_name} contains overlapping monitored column {column}"
                )));
            }
        }
        Ok(())
    }

    fn validate_monitor_entry(request: &Value, allow_where: bool) -> Result<(), Error> {
        let obj = Self::monitor_request_object(request)?;
        Self::validate_monitor_request_keys(obj, allow_where)?;
        Self::validate_monitor_request_columns(obj)?;
        Self::validate_monitor_request_select(obj)?;
        Ok(())
    }

    fn monitor_request_object(request: &Value) -> Result<&Map<String, Value>, Error> {
        request
            .as_object()
            .ok_or_else(|| Error::Validation("monitor request MUST be an object".into()))
    }

    fn validate_monitor_request_keys(
        obj: &Map<String, Value>,
        allow_where: bool,
    ) -> Result<(), Error> {
        for key in obj.keys() {
            match key.as_str() {
                "columns" | "select" => {}
                "where" if allow_where => {}
                other => {
                    return Err(Error::Validation(format!(
                        "monitor request contains unexpected field {other}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_monitor_request_columns(obj: &Map<String, Value>) -> Result<(), Error> {
        let Some(columns) = obj.get("columns") else {
            return Ok(());
        };
        let columns = columns
            .as_array()
            .ok_or_else(|| Error::Validation("monitor columns MUST be an array".into()))?;
        let mut seen = HashSet::new();
        for column in columns {
            let name = column
                .as_str()
                .ok_or_else(|| Error::Validation("monitor columns MUST contain strings".into()))?;
            if !Self::is_id(name) {
                return Err(Error::Validation(format!(
                    "invalid monitor column name {name}"
                )));
            }
            if !seen.insert(name) {
                return Err(Error::Validation(format!(
                    "duplicate monitor column {name}"
                )));
            }
        }
        Ok(())
    }

    fn validate_monitor_request_select(obj: &Map<String, Value>) -> Result<(), Error> {
        let Some(select) = obj.get("select") else {
            return Ok(());
        };
        let select = select
            .as_object()
            .ok_or_else(|| Error::Validation("monitor select MUST be an object".into()))?;
        Self::validate_monitor_select_keys(select)?;
        for field in ["initial", "insert", "delete", "modify"] {
            Self::validate_monitor_select_field(select, field)?;
        }
        Ok(())
    }

    fn validate_monitor_select_keys(select: &Map<String, Value>) -> Result<(), Error> {
        for key in select.keys() {
            match key.as_str() {
                "initial" | "insert" | "delete" | "modify" => {}
                other => {
                    return Err(Error::Validation(format!(
                        "monitor select contains unexpected field {other}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_monitor_select_field(
        select: &Map<String, Value>,
        field: &str,
    ) -> Result<(), Error> {
        let Some(value) = select.get(field) else {
            return Ok(());
        };
        if value.is_boolean() {
            Ok(())
        } else {
            Err(Error::Validation(format!(
                "monitor select field {field} MUST be boolean"
            )))
        }
    }

    /// Validate a sequence of transaction operations.
    pub fn validate_transact_ops(ops: &[Value]) -> Result<(), Error> {
        let mut seen_uuid_names = HashSet::new();
        for op in ops {
            Self::validate_transact_op(op)?;
            if let Some(uuid_name) = Self::insert_uuid_name(op) {
                if !seen_uuid_names.insert(uuid_name.to_string()) {
                    return Err(Error::Validation(
                        "duplicate uuid-name in transaction".into(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn insert_uuid_name(op: &Value) -> Option<&str> {
        let obj = op.as_object()?;
        if obj.get("op").and_then(Value::as_str) == Some("insert") {
            obj.get("uuid-name").and_then(Value::as_str)
        } else {
            None
        }
    }

    /// Validate a single transaction operation object.
    pub fn validate_transact_op(op: &Value) -> Result<(), Error> {
        let obj = op
            .as_object()
            .ok_or_else(|| Error::Validation("transaction operation MUST be an object".into()))?;
        let op_name = obj.get("op").and_then(Value::as_str).ok_or_else(|| {
            Error::Validation("transaction operation MUST include string field op".into())
        })?;

        match op_name {
            "insert" => Self::validate_insert_transact_op(obj),
            "update" => Self::validate_update_transact_op(obj),
            "mutate" => Self::validate_mutate_transact_op(obj),
            "delete" => Self::validate_delete_transact_op(obj),
            "select" => Self::validate_select_transact_op(obj),
            "wait" => Self::validate_wait_transact_op(obj),
            "commit" => Self::validate_commit_transact_op(obj),
            "abort" => Self::reject_unexpected_fields(obj, &["op"], "abort"),
            "comment" => Self::validate_comment_transact_op(obj),
            "assert" => Self::validate_assert_transact_op(obj),
            _ => Err(Error::Validation(format!(
                "unsupported transact operation {op_name}"
            ))),
        }
    }

    fn reject_unexpected_fields(
        obj: &Map<String, Value>,
        allowed: &[&str],
        op_name: &str,
    ) -> Result<(), Error> {
        for key in obj.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(Error::Validation(format!(
                    "{op_name} operation contains unexpected field {key}"
                )));
            }
        }
        Ok(())
    }

    fn require_table(obj: &Map<String, Value>, op_name: &str) -> Result<(), Error> {
        if obj.get("table").and_then(Value::as_str).is_some() {
            Ok(())
        } else {
            Err(Error::Validation(format!(
                "{op_name} operation MUST include string field table"
            )))
        }
    }

    fn require_array(
        obj: &Map<String, Value>,
        field: &'static str,
        op_name: &str,
    ) -> Result<(), Error> {
        if obj.get(field).and_then(Value::as_array).is_some() {
            Ok(())
        } else {
            Err(Error::Validation(format!(
                "{op_name} operation field {field} MUST be an array"
            )))
        }
    }

    fn require_object(
        obj: &Map<String, Value>,
        field: &'static str,
        op_name: &str,
    ) -> Result<(), Error> {
        if obj.get(field).and_then(Value::as_object).is_some() {
            Ok(())
        } else {
            Err(Error::Validation(format!(
                "{op_name} operation field {field} MUST be an object"
            )))
        }
    }

    fn require_string(
        obj: &Map<String, Value>,
        field: &'static str,
        op_name: &str,
    ) -> Result<(), Error> {
        if obj.get(field).and_then(Value::as_str).is_some() {
            Ok(())
        } else {
            Err(Error::Validation(format!(
                "{op_name} operation field {field} MUST be a string"
            )))
        }
    }

    fn validate_insert_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(obj, &["op", "table", "row", "uuid-name"], "insert")?;
        Self::require_table(obj, "insert")?;
        Self::require_object(obj, "row", "insert")?;
        if let Some(uuid_name) = obj.get("uuid-name") {
            if !uuid_name.is_string() {
                return Err(Error::Validation(
                    "insert operation field uuid-name MUST be a string".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_update_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(obj, &["op", "table", "where", "row"], "update")?;
        Self::require_table(obj, "update")?;
        Self::require_array(obj, "where", "update")?;
        Self::require_object(obj, "row", "update")
    }

    fn validate_mutate_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(obj, &["op", "table", "where", "mutations"], "mutate")?;
        Self::require_table(obj, "mutate")?;
        Self::require_array(obj, "where", "mutate")?;
        let mutations = obj
            .get("mutations")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::Validation("mutate operation field mutations MUST be an array".into())
            })?;
        for mutation in mutations {
            Self::validate_mutation_tuple(mutation)?;
        }
        Ok(())
    }

    fn validate_delete_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(obj, &["op", "table", "where"], "delete")?;
        Self::require_table(obj, "delete")?;
        Self::require_array(obj, "where", "delete")
    }

    fn validate_select_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(obj, &["op", "table", "where", "columns"], "select")?;
        Self::require_table(obj, "select")?;
        Self::require_array(obj, "where", "select")?;
        if let Some(columns) = obj.get("columns") {
            let cols = columns.as_array().ok_or_else(|| {
                Error::Validation("select operation field columns MUST be an array".into())
            })?;
            let mut seen = HashSet::new();
            for col in cols {
                let name = col.as_str().ok_or_else(|| {
                    Error::Validation(
                        "select operation field columns MUST contain only strings".into(),
                    )
                })?;
                if !seen.insert(name) {
                    return Err(Error::Validation(format!(
                        "select operation contains duplicate column {name}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_wait_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(
            obj,
            &[
                "op", "table", "where", "columns", "until", "rows", "timeout",
            ],
            "wait",
        )?;
        Self::require_table(obj, "wait")?;
        Self::require_array(obj, "where", "wait")?;
        let cols = obj
            .get("columns")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::Validation("wait operation field columns MUST be an array".into())
            })?;
        if cols.iter().any(|v| !v.is_string()) {
            return Err(Error::Validation(
                "wait operation field columns MUST contain only strings".into(),
            ));
        }
        let until = obj.get("until").and_then(Value::as_str).ok_or_else(|| {
            Error::Validation("wait operation field until MUST be a string".into())
        })?;
        if until != "==" && until != "!=" {
            return Err(Error::Validation(
                "wait operation field until MUST be == or !=".into(),
            ));
        }
        Self::require_array(obj, "rows", "wait")?;
        if let Some(timeout) = obj.get("timeout") {
            if !timeout.is_i64() && !timeout.is_u64() {
                return Err(Error::Validation(
                    "wait operation field timeout MUST be an integer".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_commit_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(obj, &["op", "durable"], "commit")?;
        let durable = obj.get("durable").ok_or_else(|| {
            Error::Validation("commit operation field durable MUST be present".into())
        })?;
        if !durable.is_boolean() {
            return Err(Error::Validation(
                "commit operation field durable MUST be a boolean".into(),
            ));
        }
        Ok(())
    }

    fn validate_comment_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(obj, &["op", "comment"], "comment")?;
        Self::require_string(obj, "comment", "comment")
    }

    fn validate_assert_transact_op(obj: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_fields(obj, &["op", "lock"], "assert")?;
        Self::require_string(obj, "lock", "assert")
    }

    fn validate_mutation_tuple(mutation: &Value) -> Result<(), Error> {
        let arr = mutation
            .as_array()
            .ok_or_else(|| Error::Validation("mutation MUST be a 3-element array".into()))?;
        if arr.len() != 3 {
            return Err(Error::Validation(
                "mutation MUST be a 3-element array".into(),
            ));
        }
        if !arr.first().is_some_and(Value::is_string) || !arr.get(1).is_some_and(Value::is_string) {
            return Err(Error::Validation(
                "mutation column and mutator MUST be strings".into(),
            ));
        }
        let Some(mutator) = arr.get(1).and_then(Value::as_str) else {
            return Err(Error::Validation(
                "mutation column and mutator MUST be strings".into(),
            ));
        };
        match mutator {
            "+=" | "-=" | "*=" | "/=" | "%=" | "insert" | "delete" => Ok(()),
            _ => Err(Error::Validation(format!("unsupported mutator {mutator}"))),
        }
    }

    fn validate_response_shape(method: &str, params: &Value, result: &Value) -> Result<(), Error> {
        match method {
            "list_dbs" => {
                let items = result.as_array().ok_or_else(|| Error::UnexpectedResponse)?;
                if items.iter().any(|v| !v.is_string()) {
                    return Err(Error::Validation(
                        "list_dbs result MUST be an array of strings".into(),
                    ));
                }
            }
            "get_schema" if !result.is_object() => {
                return Err(Error::UnexpectedResponse);
            }
            "transact" => {
                Self::validate_transact_response(params, result)?;
            }
            "monitor" | "monitor_cond" => {
                let obj = result
                    .as_object()
                    .ok_or_else(|| Error::UnexpectedResponse)?;
                Self::validate_table_updates(obj)?;
            }
            "monitor_cancel" | "unlock"
                if !result.as_object().is_some_and(serde_json::Map::is_empty) =>
            {
                return Err(Error::Validation(format!(
                    "{method} result MUST be an empty object"
                )));
            }
            "lock" => {
                let obj = result
                    .as_object()
                    .ok_or_else(|| Error::UnexpectedResponse)?;
                let locked = obj.get("locked").and_then(Value::as_bool).ok_or_else(|| {
                    Error::Validation("lock result MUST contain boolean locked".into())
                })?;
                if obj.len() != 1 {
                    return Err(Error::Validation(
                        "lock result MUST contain only locked".into(),
                    ));
                }
                if !obj.contains_key("locked") {
                    return Err(Error::Validation("lock result MUST contain locked".into()));
                }
                let _ = locked;
            }
            "steal" => {
                let obj = result
                    .as_object()
                    .ok_or_else(|| Error::UnexpectedResponse)?;
                let locked = obj.get("locked").and_then(Value::as_bool).ok_or_else(|| {
                    Error::Validation("steal result MUST contain boolean locked".into())
                })?;
                if !locked || obj.len() != 1 {
                    return Err(Error::Validation(
                        "steal result MUST be {\"locked\":true}".into(),
                    ));
                }
            }
            "echo" if !result.is_array() => {
                return Err(Error::UnexpectedResponse);
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_transact_response(params: &Value, result: &Value) -> Result<(), Error> {
        let params = params
            .as_array()
            .ok_or_else(|| Error::Validation("transact params MUST be an array".into()))?;
        let ops = params.get(1..).unwrap_or(&[]);
        let result = result.as_array().ok_or_else(|| Error::UnexpectedResponse)?;
        Self::validate_transact_response_length(ops, result)?;
        Self::validate_transact_response_entries(ops, result)
    }

    fn validate_transact_response_length(ops: &[Value], result: &[Value]) -> Result<(), Error> {
        match result.len().cmp(&ops.len()) {
            std::cmp::Ordering::Less => Err(Error::Validation(
                "transact result MUST have len(params)-1 or len(params) elements".into(),
            )),
            std::cmp::Ordering::Greater if result.len() > ops.len() + 1 => Err(Error::Validation(
                "transact result MUST have len(params)-1 or len(params) elements".into(),
            )),
            _ => Ok(()),
        }
    }

    fn validate_transact_response_entries(ops: &[Value], result: &[Value]) -> Result<(), Error> {
        let has_trailing_commit_error = result.len() == ops.len() + 1;
        if has_trailing_commit_error {
            let last_op_name = ops
                .last()
                .and_then(Value::as_object)
                .and_then(|obj| obj.get("op"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if last_op_name != "commit" {
                return Err(Error::Validation(
                    "transact trailing error object is only allowed after commit".into(),
                ));
            }
        }

        let mut saw_failure = false;
        for (idx, value) in result.iter().enumerate() {
            if has_trailing_commit_error && idx == ops.len() {
                if !Self::is_error_object(value) {
                    return Err(Error::Validation(
                        "transact trailing commit result MUST be an error object".into(),
                    ));
                }
                continue;
            }

            let op = ops.get(idx).ok_or_else(|| Error::UnexpectedResponse)?;
            let op_name = op
                .as_object()
                .and_then(|obj| obj.get("op"))
                .and_then(Value::as_str)
                .unwrap_or("");

            if value.is_null() {
                saw_failure = true;
                continue;
            }

            if saw_failure {
                return Err(Error::Validation(
                    "transact results after the first failure MUST be null".into(),
                ));
            }

            let obj = value.as_object().ok_or_else(|| {
                Error::Validation("transact result entries MUST be objects or null".into())
            })?;

            if Self::is_error_object(value) {
                saw_failure = true;
                continue;
            }

            match op_name {
                "insert" => {
                    let uuid = obj.get("uuid").ok_or_else(|| {
                        Error::Validation("insert result MUST contain uuid".into())
                    })?;
                    if !Self::is_uuid_value(uuid) {
                        return Err(Error::Validation(
                            "insert result uuid MUST be a uuid value".into(),
                        ));
                    }
                }
                "select" if !obj.get("rows").is_some_and(Value::is_array) => {
                    return Err(Error::Validation(
                        "select result MUST contain rows array".into(),
                    ));
                }
                "update" | "delete" | "mutate" if !obj.get("count").is_some_and(Value::is_u64) => {
                    return Err(Error::Validation(format!(
                        "{op_name} result MUST contain count"
                    )));
                }
                "wait" | "comment" | "commit" | "assert" if !obj.is_empty() => {
                    return Err(Error::Validation(format!("{op_name} result MUST be empty")));
                }
                "abort" => {
                    return Err(Error::Validation(
                        "abort result MUST be an error object".into(),
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn is_error_object(value: &Value) -> bool {
        value
            .as_object()
            .and_then(|obj| obj.get("error"))
            .is_some_and(|error| !error.is_null())
    }

    pub(super) fn parse_rpc_error(value: Value) -> Result<crate::model::RpcError, Error> {
        match value {
            Value::String(error) => Ok(crate::model::RpcError {
                error,
                details: None,
                other: Map::new(),
            }),
            other => Ok(serde_json::from_value(other)?),
        }
    }

    pub(super) fn validate_table_updates(obj: &Map<String, Value>) -> Result<(), Error> {
        for (table_name, table_update) in obj {
            if !Self::is_id(table_name) {
                return Err(Error::Validation(format!(
                    "invalid table name in update: {table_name}"
                )));
            }
            let table_update = table_update
                .as_object()
                .ok_or_else(|| Error::Validation("table update MUST be an object".into()))?;
            for (row_uuid, row_update) in table_update {
                Self::validate_table_update_row(row_uuid, row_update)?;
            }
        }
        Ok(())
    }

    fn validate_table_update_row(row_uuid: &str, row_update: &Value) -> Result<(), Error> {
        if !Self::is_uuid_text(row_uuid) {
            return Err(Error::Validation(format!(
                "invalid row uuid in update: {row_uuid}"
            )));
        }
        let row_update = row_update
            .as_object()
            .ok_or_else(|| Error::Validation("row update MUST be an object".into()))?;
        Self::validate_row_update_fields(row_update)
    }

    fn validate_row_update_fields(row_update: &Map<String, Value>) -> Result<(), Error> {
        Self::reject_unexpected_row_update_fields(row_update)?;
        if row_update.is_empty() {
            return Err(Error::Validation(
                "row update MUST contain old or new".into(),
            ));
        }
        Self::validate_optional_object_field(
            row_update,
            "old",
            "row update old MUST be an object",
        )?;
        Self::validate_optional_object_field(
            row_update,
            "new",
            "row update new MUST be an object",
        )?;
        Ok(())
    }

    fn reject_unexpected_row_update_fields(row_update: &Map<String, Value>) -> Result<(), Error> {
        for key in row_update.keys() {
            match key.as_str() {
                "old" | "new" => {}
                other => {
                    return Err(Error::Validation(format!(
                        "row update contains unexpected field {other}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_optional_object_field(
        obj: &Map<String, Value>,
        field: &'static str,
        message: &'static str,
    ) -> Result<(), Error> {
        let Some(value) = obj.get(field) else {
            return Ok(());
        };
        if value.is_object() {
            Ok(())
        } else {
            Err(Error::Validation(message.into()))
        }
    }

    fn is_uuid_value(value: &Value) -> bool {
        matches!(
            value.as_array(),
            Some(arr) if arr.first() == Some(&Value::String("uuid".to_string()))
                && matches!(arr.get(1), Some(Value::String(_)))
        )
    }

    fn is_uuid_text(value: &str) -> bool {
        let bytes = value.as_bytes();
        if bytes.len() != 36 {
            return false;
        }
        for (idx, ch) in bytes.iter().enumerate() {
            let valid_hex = ch.is_ascii_hexdigit();
            if matches!(idx, 8 | 13 | 18 | 23) {
                if *ch != b'-' {
                    return false;
                }
            } else if !valid_hex {
                return false;
            }
        }
        true
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
}

impl Rpc {
    /// Decode a raw RPC response into the typed value expected by the caller.
    ///
    /// # Errors
    ///
    /// Returns `MissingField`, `UnexpectedResponse`, `RpcError`, or
    /// `Validation` when the payload does not match the RPC contract.
    pub fn decode<T>(method: &str, params: &Value, resp: T) -> Result<Value, Error>
    where
        T: Borrow<Value>,
    {
        let resp = resp.borrow();
        let obj = resp.as_object().ok_or(Error::UnexpectedResponse)?;
        if !obj.contains_key("id") {
            return Err(Error::MissingField("id"));
        }

        let error = obj.get("error").ok_or(Error::MissingField("error"))?;

        if !error.is_null() {
            let rpc_err = Validator::parse_rpc_error(error.clone())?;
            return Err(Error::RpcError(rpc_err));
        }

        let result = obj.get("result").ok_or(Error::MissingField("result"))?;

        Validator::validate_response_shape(method, params, result)?;
        Ok(result.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_encode_emits_expected_rpc_shape() {
        let value = Rpc::encode("echo", 7, json!(["ping"]));
        assert_eq!(
            value,
            json!({
                "method": "echo",
                "id": 7,
                "params": ["ping"]
            })
        );
    }

    #[test]
    fn test_encode_emits_cancel_notification_shape() {
        let value = Rpc::encode("cancel", Value::Null, json!([7]));
        assert_eq!(
            value,
            json!({
                "method": "cancel",
                "id": null,
                "params": [7]
            })
        );
    }

    #[test]
    fn test_validate_transact_ops_rejects_duplicate_uuid_names() {
        let ops = vec![
            json!({"op":"insert","table":"T","row":{},"uuid-name":"row1"}),
            json!({"op":"insert","table":"T","row":{},"uuid-name":"row1"}),
        ];
        assert!(Rpc::validate_transact_ops(&ops).is_err());
    }

    #[test]
    fn test_validate_method_params_accepts_monitor_cond_where_clause() {
        let params = json!([
            "Open_vSwitch",
            "monitor-id",
            {
                "Open_vSwitch": [
                    {
                        "columns": ["external_ids"],
                        "select": {"initial": true},
                        "where": [["name", "==", "foo"]]
                    }
                ]
            }
        ]);
        assert!(Rpc::validate_method_params("monitor_cond", &params).is_ok());
    }

    #[test]
    fn test_validate_method_params_rejects_bad_lock_arity() {
        assert!(matches!(
            Rpc::validate_method_params("lock", &json!(["id", "extra"])),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_method_params("unlock", &json!([1])),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn test_validate_table_updates_rejects_empty_row_update() {
        let update = json!({
            "Open_vSwitch": {
                "550e8400-e29b-41d4-a716-446655440000": {}
            }
        });
        assert!(update
            .as_object()
            .is_some_and(|obj| Rpc::validate_table_updates(obj).is_err()));
    }
}
