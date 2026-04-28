/// Reject strings containing null bytes.
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
