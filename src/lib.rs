mod builder;
mod client;
mod error;
mod multipart;
mod request;
mod xml;

use std::sync::Arc;

use chrono::Utc;
use futures::stream::BoxStream;
use futures::StreamExt;

pub use builder::*;
pub use error::AiStoreError;

use crate::multipart::AiStoreMultipartUpload;

#[derive(Debug, Clone)]
pub struct AiStore {
    client: Arc<client::S3Client>,
    bucket_name: String,
}

impl std::fmt::Display for AiStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AiStore({})", self.bucket_name)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for AiStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        _opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.client
            .put_object(location, payload)
            .await
            .map_err(Into::into)
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        _opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        let upload_id = self
            .client
            .initiate_multipart_upload(location)
            .await
            .map_err(object_store::Error::from)?;

        Ok(Box::new(AiStoreMultipartUpload::new(
            self.client.clone(),
            location.clone(),
            upload_id,
        )))
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.client
            .get_object(location, options)
            .await
            .map_err(Into::into)
    }

    async fn head(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<object_store::ObjectMeta> {
        self.client.head_object(location).await.map_err(Into::into)
    }

    async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        self.client
            .delete_object(location)
            .await
            .map_err(Into::into)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        let client = self.client.clone();
        let prefix = prefix.map(|p| p.to_string());

        futures::stream::unfold(
            ListState {
                client,
                prefix,
                continuation_token: None,
                done: false,
                buffer: vec![],
            },
            |mut state| async move {
                if state.done && state.buffer.is_empty() {
                    return None;
                }

                if let Some(item) = state.buffer.pop() {
                    return Some((Ok(item), state));
                }

                let result = state
                    .client
                    .list_objects(
                        state.prefix.as_deref(),
                        state.continuation_token.as_deref(),
                        Some(1000),
                    )
                    .await;

                match result {
                    Ok(response) => {
                        let is_truncated = response.is_truncated.unwrap_or(false);
                        if !is_truncated || response.next_continuation_token.is_none() {
                            state.done = true;
                        } else {
                            state.continuation_token = response.next_continuation_token;
                        }

                        state.buffer = response
                            .contents
                            .into_iter()
                            .filter_map(|entry| {
                                let location = object_store::path::Path::parse(&entry.key).ok()?;
                                Some(object_store::ObjectMeta {
                                    location,
                                    last_modified: entry.last_modified.unwrap_or_else(Utc::now),
                                    size: entry.size,
                                    e_tag: entry.e_tag,
                                    version: None,
                                })
                            })
                            .collect();

                        state.buffer.reverse();

                        if let Some(item) = state.buffer.pop() {
                            Some((Ok(item), state))
                        } else {
                            None
                        }
                    }
                    Err(e) => {
                        state.done = true;
                        Some((Err(e.into()), state))
                    }
                }
            },
        )
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        // AIStore doesn't have native delimiter support in the same way as S3
        // We simulate it by listing all objects and grouping them
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();

        let mut objects = vec![];
        let mut common_prefixes = std::collections::HashSet::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let response = self
                .client
                .list_objects(Some(&prefix_str), continuation_token.as_deref(), Some(1000))
                .await
                .map_err(object_store::Error::from)?;

            for entry in response.contents {
                let name = &entry.key;

                let relative_path = if prefix_str.is_empty() {
                    name.as_str()
                } else if let Some(stripped) = name.strip_prefix(&prefix_str) {
                    stripped.trim_start_matches('/')
                } else {
                    continue;
                };

                if let Some(slash_pos) = relative_path.find('/') {
                    let dir_prefix = if prefix_str.is_empty() {
                        format!("{}/", &relative_path[..slash_pos])
                    } else {
                        format!(
                            "{}/{}/",
                            prefix_str.trim_end_matches('/'),
                            &relative_path[..slash_pos]
                        )
                    };
                    common_prefixes.insert(dir_prefix);
                } else if let Ok(location) = object_store::path::Path::parse(&entry.key) {
                    objects.push(object_store::ObjectMeta {
                        location,
                        last_modified: entry.last_modified.unwrap_or_else(Utc::now),
                        size: entry.size,
                        e_tag: entry.e_tag,
                        version: None,
                    });
                }
            }

            let is_truncated = response.is_truncated.unwrap_or(false);
            if !is_truncated || response.next_continuation_token.is_none() {
                break;
            }
            continuation_token = response.next_continuation_token;
        }

        Ok(object_store::ListResult {
            objects,
            common_prefixes: common_prefixes
                .into_iter()
                .filter_map(|p| object_store::path::Path::parse(&p).ok())
                .collect(),
        })
    }

    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.client.copy_object(from, to).await.map_err(Into::into)
    }

    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        // Check if destination exists first
        match self.head(to).await {
            Ok(_) => Err(object_store::Error::AlreadyExists {
                path: to.to_string(),
                source: Box::new(AiStoreError::AlreadyExists {
                    message: format!("Object already exists: {}", to),
                }),
            }),
            Err(object_store::Error::NotFound { .. }) => {
                // Object doesn't exist, proceed with copy
                self.copy(from, to).await
            }
            Err(e) => Err(e),
        }
    }
}

struct ListState {
    client: Arc<client::S3Client>,
    prefix: Option<String>,
    continuation_token: Option<String>,
    done: bool,
    buffer: Vec<object_store::ObjectMeta>,
}
