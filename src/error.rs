use object_store::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum AiStoreError {
    #[error("Object not found: {message}")]
    NotFound { message: String },

    #[error("Access forbidden: {message}")]
    Forbidden { message: String },

    #[error("Unauthorized: {message}")]
    Unauthorized { message: String },

    #[error("Object already exists: {message}")]
    AlreadyExists { message: String },

    #[error("HTTP error {status}: {message}")]
    Http { status: u16, message: String },

    #[error("Request error: {source}")]
    Request {
        #[source]
        source: reqwest::Error,
    },

    #[error("Invalid response: {message}")]
    InvalidResponse { message: String },

    #[error("Not modified: {path}")]
    NotModified { path: Path },

    #[error("Precondition failed: {path}")]
    PreconditionFailed { path: Path },

    #[error("Configuration error: {message}")]
    Configuration { message: String },
}

impl From<AiStoreError> for object_store::Error {
    fn from(err: AiStoreError) -> Self {
        match &err {
            AiStoreError::NotFound { message } => object_store::Error::NotFound {
                path: message.clone(),
                source: Box::new(err),
            },
            AiStoreError::AlreadyExists { message } => object_store::Error::AlreadyExists {
                path: message.clone(),
                source: Box::new(err),
            },
            AiStoreError::NotModified { path } => object_store::Error::NotModified {
                path: path.to_string(),
                source: Box::new(err),
            },
            AiStoreError::PreconditionFailed { path } => object_store::Error::Precondition {
                path: path.to_string(),
                source: Box::new(err),
            },
            _ => object_store::Error::Generic {
                store: "aistore",
                source: Box::new(err),
            },
        }
    }
}
