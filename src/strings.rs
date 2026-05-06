/// Reject strings containing null bytes.
///
/// # Examples
///
/// ```rust
/// use ovsdb::strings::reject_null_bytes;
///
/// assert!(reject_null_bytes("tcp:127.0.0.1:6640").is_ok());
/// assert!(reject_null_bytes("bad\0value").is_err());
/// ```
///
/// # Errors
///
/// Returns `Err` when the string contains a null byte.
pub fn reject_null_bytes(val: &str) -> Result<(), &'static str> {
    if val.contains('\0') {
        Err("string value contains forbidden null byte")
    } else {
        Ok(())
    }
}
