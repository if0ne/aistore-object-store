use std::time::Duration;

use futures::StreamExt;
use object_store::PutPayload;
use reqwest::{Body, Client, Method, Response, StatusCode};

use crate::error::AiStoreError;

/// Configuration for retry and redirect behavior
#[derive(Debug, Clone)]
pub struct RequestPolicy {
    /// Maximum number of retry attempts for transient errors
    pub max_retries: u32,
    /// Maximum number of redirects to follow
    pub max_redirects: u32,
    /// Initial delay between retries (will be multiplied by backoff_factor)
    pub initial_retry_delay: Duration,
    /// Multiplier for retry delay on each attempt
    pub backoff_factor: f64,
    /// Maximum delay between retries
    pub max_retry_delay: Duration,
}

impl Default for RequestPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            max_redirects: 10,
            initial_retry_delay: Duration::from_millis(100),
            backoff_factor: 2.0,
            max_retry_delay: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RequestBody {
    Bytes(bytes::Bytes),
    Payload(PutPayload),
    Text(String),
}

impl Into<Body> for RequestBody {
    fn into(self) -> Body {
        match self {
            RequestBody::Bytes(bytes) => Body::from(bytes),
            RequestBody::Payload(payload) => {
                let stream = futures::stream::iter(payload).map(Ok::<_, std::io::Error>);
                Body::wrap_stream(stream)
            }
            RequestBody::Text(text) => Body::from(text),
        }
    }
}

/// Builder for HTTP requests with retry and redirect handling
pub struct HttpRequestBuilder {
    client: Client,
    method: Method,
    url: String,
    body: Option<RequestBody>,
    headers: Vec<(String, String)>,
    query_params: Vec<(String, String)>,
    policy: RequestPolicy,
}

impl HttpRequestBuilder {
    pub fn new(client: Client, method: Method, url: impl Into<String>) -> Self {
        Self {
            client,
            method,
            url: url.into(),
            body: None,
            headers: Vec::new(),
            query_params: Vec::new(),
            policy: RequestPolicy::default(),
        }
    }

    /// Set the request body
    pub fn body(mut self, body: RequestBody) -> Self {
        self.body = Some(body);
        self
    }

    /// Add a header to the request
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Add a query parameter
    pub fn query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.query_params.push((name.into(), value.into()));
        self
    }

    /// Add multiple query parameters
    pub fn query_params(mut self, params: Vec<(String, String)>) -> Self {
        self.query_params.extend(params);
        self
    }

    /// Set the retry/redirect policy
    pub fn policy(mut self, policy: RequestPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Send the request with retry and redirect handling
    pub async fn send(mut self) -> Result<Response, AiStoreError> {
        let mut redirects = 0;
        let mut retries = 0;
        let mut retry_delay = self.policy.initial_retry_delay;

        loop {
            let result = self.send_once().await;

            match result {
                Ok(response) => {
                    let status = response.status();

                    // Handle redirects (301, 302, 307, 308)
                    if status.is_redirection() {
                        if redirects >= self.policy.max_redirects {
                            return Err(AiStoreError::InvalidResponse {
                                message: format!(
                                    "Too many redirects (max: {})",
                                    self.policy.max_redirects
                                ),
                            });
                        }

                        if let Some(location) = response.headers().get("location") {
                            if let Ok(location_str) = location.to_str() {
                                self.url = location_str.to_string();
                                redirects += 1;
                                continue;
                            }
                        }

                        return Err(AiStoreError::InvalidResponse {
                            message: format!(
                                "{} redirect without Location header",
                                status.as_u16()
                            ),
                        });
                    }

                    // Check for retryable status codes
                    if Self::is_retryable_status(status) && retries < self.policy.max_retries {
                        retries += 1;
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = self.next_retry_delay(retry_delay);
                        continue;
                    }

                    return Ok(response);
                }
                Err(e) => {
                    // Retry on transient network errors
                    if Self::is_retryable_error(&e) && retries < self.policy.max_retries {
                        retries += 1;
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = self.next_retry_delay(retry_delay);
                        continue;
                    }

                    return Err(e);
                }
            }
        }
    }

    /// Send a single request without retry logic
    async fn send_once(&mut self) -> Result<Response, AiStoreError> {
        let mut request = self.client.request(self.method.clone(), &self.url);

        // Add query parameters
        if !self.query_params.is_empty() {
            request = request.query(&self.query_params);
        }

        // Add headers
        for (name, value) in &self.headers {
            request = request.header(name.as_str(), value.as_str());
        }

        // Add body if present (take ownership since Body is not Clone)
        if let Some(body) = self.body.clone().take() {
            request = request.body(body);
        }

        request
            .send()
            .await
            .map_err(|e| AiStoreError::Request { source: e })
    }

    /// Calculate the next retry delay with exponential backoff
    fn next_retry_delay(&self, current: Duration) -> Duration {
        let next = Duration::from_secs_f64(current.as_secs_f64() * self.policy.backoff_factor);
        next.min(self.policy.max_retry_delay)
    }

    /// Check if a status code is retryable
    fn is_retryable_status(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::REQUEST_TIMEOUT
                | StatusCode::TOO_MANY_REQUESTS
                | StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        )
    }

    /// Check if an error is retryable (transient network errors)
    fn is_retryable_error(error: &AiStoreError) -> bool {
        match error {
            AiStoreError::Request { source } => {
                source.is_timeout() || source.is_connect() || source.is_request()
            }
            _ => false,
        }
    }
}

pub trait ClientExt {
    fn get_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder;
    fn put_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder;
    fn post_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder;
    fn delete_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder;
    fn head_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder;
}

impl ClientExt for Client {
    fn get_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder {
        HttpRequestBuilder::new(self.clone(), Method::GET, url)
    }

    fn put_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder {
        HttpRequestBuilder::new(self.clone(), Method::PUT, url)
    }

    fn post_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder {
        HttpRequestBuilder::new(self.clone(), Method::POST, url)
    }

    fn delete_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder {
        HttpRequestBuilder::new(self.clone(), Method::DELETE, url)
    }

    fn head_with_retry(&self, url: impl Into<String>) -> HttpRequestBuilder {
        HttpRequestBuilder::new(self.clone(), Method::HEAD, url)
    }
}
