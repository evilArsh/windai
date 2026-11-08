use rmcp::model::ErrorData;

use crate::builtin::paths::PathError;

impl From<PathError> for ErrorData {
    fn from(error: PathError) -> Self {
        ErrorData::internal_error(error.to_string(), None)
    }
}
