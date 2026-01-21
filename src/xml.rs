//! XML response types for S3-compatible API

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Response from S3 ListObjectsV2 API
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListBucketResult {
    #[serde(default)]
    pub contents: Vec<ListContents>,
    #[serde(default)]
    pub common_prefixes: Vec<ListPrefix>,
    #[serde(default)]
    pub next_continuation_token: Option<String>,
    #[serde(default)]
    pub is_truncated: Option<bool>,
}

/// Object entry in ListObjectsV2 response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListContents {
    pub key: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub last_modified: Option<DateTime<Utc>>,
    #[serde(rename = "ETag", default)]
    pub e_tag: Option<String>,
}

/// Common prefix entry in ListObjectsV2 response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPrefix {
    pub prefix: String,
}

/// Response from InitiateMultipartUpload
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    pub upload_id: String,
}

/// Request body for CompleteMultipartUpload
#[derive(Debug, Serialize)]
#[serde(rename = "CompleteMultipartUpload")]
pub struct CompleteMultipartUploadRequest {
    #[serde(rename = "Part")]
    pub parts: Vec<CompletedPart>,
}

/// A part in CompleteMultipartUpload request
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompletedPart {
    pub part_number: u32,
    #[serde(rename = "ETag")]
    pub e_tag: String,
}

impl CompleteMultipartUploadRequest {
    pub fn new(parts: Vec<(u32, String)>) -> Self {
        Self {
            parts: parts
                .into_iter()
                .map(|(part_number, e_tag)| CompletedPart { part_number, e_tag })
                .collect(),
        }
    }
}

/// Parse XML response using quick-xml
pub fn from_xml<'de, T: Deserialize<'de>>(xml: &'de str) -> Result<T, quick_xml::DeError> {
    quick_xml::de::from_str(xml)
}

/// Serialize to XML using quick-xml
pub fn to_xml<T: Serialize>(value: &T) -> Result<String, quick_xml::SeError> {
    quick_xml::se::to_string(value)
}
