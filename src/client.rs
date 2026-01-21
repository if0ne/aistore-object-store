use std::ops::Range;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::{
    path::Path, GetOptions, GetRange, GetResult, GetResultPayload, ObjectMeta, PutPayload,
    PutResult,
};
use reqwest::{Response, StatusCode};

use crate::error::AiStoreError;
use crate::request::{ClientExt, RequestBody};
use crate::xml::{self, CompleteMultipartUploadRequest, ListBucketResult};

#[derive(Debug, Clone)]
pub(crate) struct S3Config {
    pub url: String,
}

#[derive(Debug, Clone)]
pub(crate) struct S3Client {
    config: S3Config,
    client: reqwest::Client,
}

impl S3Client {
    pub(crate) fn new(config: S3Config, client: reqwest::Client) -> Self {
        Self { config, client }
    }

    fn object_url(&self, path: &Path) -> String {
        format!("{}/{}", self.config.url, path.as_ref())
    }

    fn bucket_url(&self) -> &str {
        &self.config.url
    }

    pub(crate) async fn put_object(
        &self,
        path: &Path,
        payload: PutPayload,
    ) -> Result<PutResult, AiStoreError> {
        let url = self.object_url(path);
        let content_length = payload.content_length();

        let response = self
            .client
            .put_with_retry(url)
            .header(
                reqwest::header::CONTENT_LENGTH.as_str(),
                content_length.to_string(),
            )
            .body(RequestBody::Payload(payload))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());

        let version = response
            .headers()
            .get("x-ais-version")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(PutResult {
            e_tag: etag,
            version,
        })
    }

    pub(crate) async fn get_object(
        &self,
        path: &Path,
        options: GetOptions,
    ) -> Result<GetResult, AiStoreError> {
        let url = self.object_url(path);

        let mut request = if options.head {
            self.client.head_with_retry(&url)
        } else {
            self.client.get_with_retry(&url)
        };

        if let Some(range) = &options.range {
            let range_header = match range {
                GetRange::Bounded(r) => format!("bytes={}-{}", r.start, r.end.saturating_sub(1)),
                GetRange::Offset(offset) => format!("bytes={}-", offset),
                GetRange::Suffix(length) => format!("bytes=-{}", length),
            };
            request = request.header(reqwest::header::RANGE.to_string(), range_header);
        }

        if let Some(if_match) = &options.if_match {
            request = request.header(
                reqwest::header::IF_MATCH.to_string(),
                if_match.as_ref() as &str,
            );
        }

        if let Some(if_none_match) = &options.if_none_match {
            request = request.header(
                reqwest::header::IF_NONE_MATCH.to_string(),
                if_none_match.as_ref() as &str,
            );
        }

        if let Some(if_modified_since) = &options.if_modified_since {
            request = request.header(
                reqwest::header::IF_MODIFIED_SINCE.to_string(),
                if_modified_since
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string(),
            );
        }

        if let Some(if_unmodified_since) = &options.if_unmodified_since {
            request = request.header(
                reqwest::header::IF_UNMODIFIED_SINCE.to_string(),
                if_unmodified_since
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string(),
            );
        }

        let response = request.send().await?;
        let status = response.status();

        if status == StatusCode::NOT_MODIFIED {
            return Err(AiStoreError::NotModified { path: path.clone() });
        }

        if status == StatusCode::PRECONDITION_FAILED {
            return Err(AiStoreError::PreconditionFailed { path: path.clone() });
        }

        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        let meta = Self::extract_object_meta(path, &response)?;
        let content_range = Self::parse_content_range(&response);

        let range = content_range.unwrap_or(0..meta.size);

        if options.head {
            Ok(GetResult {
                meta,
                range,
                attributes: Default::default(),
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
            })
        } else {
            let stream = response.bytes_stream();
            let stream = stream.map_err(|e| object_store::Error::Generic {
                store: "aistore",
                source: Box::new(e),
            });

            Ok(GetResult {
                meta,
                range,
                attributes: Default::default(),
                payload: GetResultPayload::Stream(Box::pin(stream)),
            })
        }
    }

    pub(crate) async fn head_object(&self, path: &Path) -> Result<ObjectMeta, AiStoreError> {
        let url = self.object_url(path);

        let response = self.client.head_with_retry(url).send().await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        Self::extract_object_meta(path, &response)
    }

    pub(crate) async fn delete_object(&self, path: &Path) -> Result<(), AiStoreError> {
        let url = self.object_url(path);

        let response = self.client.delete_with_retry(url).send().await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        Ok(())
    }

    pub(crate) async fn list_objects(
        &self,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: Option<u32>,
    ) -> Result<ListBucketResult, AiStoreError> {
        let url = self.bucket_url();

        let mut query_params = vec![("list-type".to_string(), "2".to_string())];

        if let Some(prefix) = prefix {
            query_params.push(("prefix".to_string(), prefix.to_string()));
        }

        if let Some(token) = continuation_token {
            query_params.push(("continuation-token".to_string(), token.to_string()));
        }

        if let Some(max) = max_keys {
            query_params.push(("max-keys".to_string(), max.to_string()));
        }

        let response = self
            .client
            .get_with_retry(url)
            .query_params(query_params)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        let body = response
            .text()
            .await
            .map_err(|e| AiStoreError::Request { source: e })?;

        xml::from_xml(&body).map_err(|e| AiStoreError::InvalidResponse {
            message: format!("Failed to parse ListObjectsV2 response: {}", e),
        })
    }

    pub(crate) async fn copy_object(&self, from: &Path, to: &Path) -> Result<(), AiStoreError> {
        let url = self.object_url(to);
        let source = self.object_url(from);

        let response = self
            .client
            .put_with_retry(url)
            .header("x-amz-copy-source", source)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        Ok(())
    }

    pub(crate) async fn initiate_multipart_upload(
        &self,
        path: &Path,
    ) -> Result<String, AiStoreError> {
        let url = format!("{}?uploads", self.object_url(path));

        let response = self.client.post_with_retry(url).send().await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        if let Some(upload_id) = response.headers().get("x-ais-upload-id") {
            return upload_id.to_str().map(|s| s.to_string()).map_err(|_| {
                AiStoreError::InvalidResponse {
                    message: "Invalid upload ID header".to_string(),
                }
            });
        }

        let body = response
            .text()
            .await
            .map_err(|e| AiStoreError::Request { source: e })?;

        let result: xml::InitiateMultipartUploadResult =
            xml::from_xml(&body).map_err(|e| AiStoreError::InvalidResponse {
                message: format!("Failed to parse InitiateMultipartUpload response: {}", e),
            })?;

        Ok(result.upload_id)
    }

    pub(crate) async fn upload_part(
        &self,
        path: &Path,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<String, AiStoreError> {
        let url = format!(
            "{}?partNumber={}&uploadId={}",
            self.object_url(path),
            part_number,
            upload_id
        );

        let response = self
            .client
            .put_with_retry(url)
            .body(RequestBody::Bytes(data))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
            .ok_or_else(|| AiStoreError::InvalidResponse {
                message: "No ETag in part upload response".to_string(),
            })?;

        Ok(etag)
    }

    pub(crate) async fn complete_multipart_upload(
        &self,
        path: &Path,
        upload_id: &str,
        parts: Vec<(u32, String)>,
    ) -> Result<PutResult, AiStoreError> {
        let url = format!("{}?uploadId={}", self.object_url(path), upload_id);

        let request_body = CompleteMultipartUploadRequest::new(parts);
        let xml = xml::to_xml(&request_body).map_err(|e| AiStoreError::InvalidResponse {
            message: format!("Failed to serialize CompleteMultipartUpload request: {}", e),
        })?;

        let response = self
            .client
            .post_with_retry(url)
            .header(reqwest::header::CONTENT_TYPE.to_string(), "application/xml")
            .body(RequestBody::Text(xml))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());

        let version = response
            .headers()
            .get("x-ais-version")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(PutResult {
            e_tag: etag,
            version,
        })
    }

    /// Abort a multipart upload
    pub(crate) async fn abort_multipart_upload(
        &self,
        path: &Path,
        upload_id: &str,
    ) -> Result<(), AiStoreError> {
        let url = format!("{}?uploadId={}", self.object_url(path), upload_id);

        let response = self.client.delete_with_retry(url).send().await?;

        let status = response.status();
        if !status.is_success() {
            return Err(Self::handle_error_response(response).await);
        }

        Ok(())
    }

    fn extract_object_meta(path: &Path, response: &Response) -> Result<ObjectMeta, AiStoreError> {
        let headers = response.headers();

        let size = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let last_modified = headers
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        let e_tag = headers
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());

        let version = headers
            .get("x-ais-version")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(ObjectMeta {
            location: path.clone(),
            last_modified,
            size,
            e_tag,
            version,
        })
    }

    fn parse_content_range(response: &Response) -> Option<Range<u64>> {
        let content_range = response.headers().get(reqwest::header::CONTENT_RANGE)?;
        let content_range = content_range.to_str().ok()?;

        let parts: Vec<&str> = content_range.split(' ').collect();
        if parts.len() != 2 || parts[0] != "bytes" {
            return None;
        }

        let range_parts: Vec<&str> = parts[1].split('/').collect();
        if range_parts.is_empty() {
            return None;
        }

        let byte_range: Vec<&str> = range_parts[0].split('-').collect();
        if byte_range.len() != 2 {
            return None;
        }

        let start = byte_range[0].parse::<u64>().ok()?;
        let end = byte_range[1].parse::<u64>().ok()? + 1;

        Some(start..end)
    }

    async fn handle_error_response(response: Response) -> AiStoreError {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        match status {
            StatusCode::NOT_FOUND => AiStoreError::NotFound { message: body },
            StatusCode::FORBIDDEN => AiStoreError::Forbidden { message: body },
            StatusCode::UNAUTHORIZED => AiStoreError::Unauthorized { message: body },
            StatusCode::CONFLICT => AiStoreError::AlreadyExists { message: body },
            _ => AiStoreError::Http {
                status: status.as_u16(),
                message: body,
            },
        }
    }
}
