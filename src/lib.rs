mod builder;
mod client;

use std::sync::Arc;

pub use builder::*;

#[derive(Debug, Clone)]
pub struct AiStore {
    client: Arc<client::Client>,
}

impl std::fmt::Display for AiStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ais()")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for AiStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        todo!()
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        todo!()
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        todo!()
    }

    async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        todo!()
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        todo!()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        todo!()
    }

    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        todo!()
    }

    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        todo!()
    }
}
