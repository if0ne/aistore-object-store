use std::sync::Arc;
use std::time::Duration;

use crate::{
    client::{S3Client, S3Config},
    AiStore,
};

#[derive(Default)]
pub struct AiStoreBuilder {
    endpoint: Option<String>,
    bucket_name: Option<String>,
    auth_jwt_token: Option<String>,
    allow_http: bool,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    s3_api_via_root: bool,
}

impl AiStoreBuilder {
    pub fn new() -> Self {
        AiStoreBuilder::default()
    }

    /// Set the AIStore endpoint URL (e.g., "aistore.example.com" or "localhost:8080")
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the bucket name to use
    pub fn with_bucket_name(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket_name.into());
        self
    }

    /// Set the JWT authentication token
    pub fn with_auth_jwt_token(mut self, auth_jwt_token: impl Into<String>) -> Self {
        self.auth_jwt_token = Some(auth_jwt_token.into());
        self
    }

    /// Allow HTTP connections (default: false, HTTPS only)
    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = allow_http;
        self
    }

    /// Set the request timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Configure S3 API path routing.
    ///
    /// When `true`, the S3 API is served at the root: `http(s)://{endpoint}/{bucket}`
    /// When `false` (default), the S3 API is served at: `http(s)://{endpoint}/s3/{bucket}`
    pub fn with_s3_api_via_root(mut self, s3_api_via_root: bool) -> Self {
        self.s3_api_via_root = s3_api_via_root;
        self
    }

    /// Build the AiStore client
    pub fn build(self) -> object_store::Result<AiStore> {
        let bucket = self.bucket_name.ok_or(BuilderError::MissingBucketName)?;
        let endpoint = self.endpoint.ok_or(BuilderError::MissingEndpoint)?;

        let url = if self.s3_api_via_root {
            format!("{endpoint}/{bucket}")
        } else {
            format!("{endpoint}/s3/{bucket}")
        };

        let mut client_builder = reqwest::Client::builder();

        if let Some(timeout) = self.timeout {
            client_builder = client_builder.timeout(timeout);
        }

        if let Some(connect_timeout) = self.connect_timeout {
            client_builder = client_builder.connect_timeout(connect_timeout);
        }

        let mut headers = reqwest::header::HeaderMap::new();

        if let Some(jwt) = &self.auth_jwt_token {
            let auth_value = reqwest::header::HeaderValue::from_str(&format!("Bearer {jwt}"))
                .map_err(|e| BuilderError::InvalidAuthToken {
                    message: e.to_string(),
                })?;
            headers.insert(reqwest::header::AUTHORIZATION, auth_value);
        }

        client_builder = client_builder
            .default_headers(headers)
            .https_only(!self.allow_http);

        let http_client = client_builder
            .build()
            .map_err(|e| BuilderError::HttpClient { source: e })?;

        let client_config = S3Config { url };
        let client = Arc::new(S3Client::new(client_config, http_client));

        Ok(AiStore {
            client,
            bucket_name: bucket,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("Missing bucket name")]
    MissingBucketName,

    #[error("Missing endpoint")]
    MissingEndpoint,

    #[error("Invalid auth token: {message}")]
    InvalidAuthToken { message: String },

    #[error("Failed to build HTTP client: {source}")]
    HttpClient {
        #[source]
        source: reqwest::Error,
    },
}

impl From<BuilderError> for object_store::Error {
    fn from(source: BuilderError) -> Self {
        Self::Generic {
            store: "aistore",
            source: Box::new(source),
        }
    }
}
