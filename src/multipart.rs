use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, MultipartUpload, PutPayload, PutResult};
use tokio::sync::Mutex;

use crate::client::S3Client;

pub struct AiStoreMultipartUpload {
    client: Arc<S3Client>,
    location: Path,
    upload_id: String,
    state: Arc<Mutex<MultipartState>>,
}

struct MultipartState {
    parts: Vec<(u32, String)>,
    next_part_number: u32,
}

impl AiStoreMultipartUpload {
    pub fn new(client: Arc<S3Client>, location: Path, upload_id: String) -> Self {
        Self {
            client,
            location,
            upload_id,
            state: Arc::new(Mutex::new(MultipartState {
                parts: Vec::new(),
                next_part_number: 1,
            })),
        }
    }
}

#[async_trait::async_trait]
impl MultipartUpload for AiStoreMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let client = self.client.clone();
        let location = self.location.clone();
        let upload_id = self.upload_id.clone();
        let state = self.state.clone();

        Box::pin(async move {
            let part_number = {
                let mut state = state.lock().await;
                let num = state.next_part_number;
                state.next_part_number += 1;
                num
            };

            let mut bytes = Vec::new();
            for chunk in data {
                bytes.extend_from_slice(&chunk);
            }
            let data = Bytes::from(bytes);

            let etag = client
                .upload_part(&location, &upload_id, part_number, data)
                .await
                .map_err(object_store::Error::from)?;

            {
                let mut state = state.lock().await;
                state.parts.push((part_number, etag));
            }

            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = {
            let state = self.state.lock().await;
            let mut parts = state.parts.clone();
            parts.sort_by_key(|(num, _)| *num);
            parts
        };

        self.client
            .complete_multipart_upload(&self.location, &self.upload_id, parts)
            .await
            .map_err(Into::into)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.client
            .abort_multipart_upload(&self.location, &self.upload_id)
            .await
            .map_err(Into::into)
    }
}

impl std::fmt::Debug for AiStoreMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiStoreMultipartUpload")
            .field("location", &self.location)
            .field("upload_id", &self.upload_id)
            .finish()
    }
}
