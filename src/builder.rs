use std::sync::Arc;

use object_store::client::HttpConnector;

use crate::{
    client::{Client, ClientConfig},
    AiStore,
};

#[derive(Default)]
pub struct AiStoreBuilder {
    client_options: object_store::ClientOptions,
    endpoint: Option<String>,
    bucket_name: Option<String>,
    max_redirects: Option<usize>,
    auth_jwt_token: Option<String>,
    s3_api_via_root: bool,
}

impl AiStoreBuilder {
    pub fn new() -> Self {
        AiStoreBuilder::default()
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn with_bucket_name(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket_name.into());
        self
    }

    pub fn with_auth_jwt_token(mut self, auth_jwt_token: impl Into<String>) -> Self {
        self.auth_jwt_token = Some(auth_jwt_token.into());
        self
    }

    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.client_options = self.client_options.with_allow_http(allow_http);
        self
    }

    pub fn with_max_redirects(mut self, max_redirects: usize) -> Self {
        self.max_redirects = Some(max_redirects);
        self
    }

    pub fn with_s3_api_via_root(mut self, s3_api_via_root: bool) -> Self {
        self.s3_api_via_root = s3_api_via_root;
        self
    }

    pub fn build(mut self) -> object_store::Result<AiStore> {
        let http = Arc::new(object_store::client::ReqwestConnector {});

        let bucket = self.bucket_name.ok_or(BuilderError::MissingBucketName)?;
        let endpoint = self.endpoint.ok_or(BuilderError::MissingEndpoint)?;

        let url = if self.s3_api_via_root {
            format!("https://{endpoint}/{bucket}")
        } else {
            format!("https://{endpoint}/s3/{bucket}")
        };

        let header_map = self
            .auth_jwt_token
            .as_ref()
            .and_then(|jwt| {
                let mut header_map = object_store::HeaderMap::new();
                header_map.insert(
                    "Authorization",
                    object_store::HeaderValue::from_str(&format!("Bearer {jwt}")).ok()?,
                );
                Some(header_map)
            })
            .unwrap_or_default();

        self.client_options = self.client_options.with_default_headers(header_map);

        let http_client = http.connect(&self.client_options)?;

        let client_config = ClientConfig { url };
        let client = Arc::new(Client::new(client_config, http_client));

        Ok(AiStore { client })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("Missing bucket name")]
    MissingBucketName,

    #[error("Missing endpoint")]
    MissingEndpoint,
}

impl From<BuilderError> for object_store::Error {
    fn from(source: BuilderError) -> Self {
        Self::Generic {
            store: "aistore",
            source: Box::new(source),
        }
    }
}
