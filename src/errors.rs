use std::fmt;
use std::fmt::Formatter;

#[derive(Debug)]
pub enum EngineError {
    ResultToLargeError,
    TypeError {
        operation: &'static str,
        field: String,
    },
    FieldNotFound(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::ResultToLargeError => {
                write!(f, "Too many results, maximum 5000")
            }
            EngineError::TypeError { operation, field } => {
                write!(f, "Invalid operation {} for {}", operation, field)
            }
            EngineError::FieldNotFound(s) => write!(f, "Invalid field {}", s),
        }
    }
}

impl std::error::Error for EngineError {}
