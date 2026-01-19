#[derive(Debug)]
pub(crate) struct ClientConfig {
    pub url: String,
}

#[derive(Debug)]
pub(crate) struct Client {
    config: ClientConfig,
    client: object_store::client::HttpClient,
}

impl Client {
    pub(crate) fn new(config: ClientConfig, client: object_store::client::HttpClient) -> Self {
        Self { config, client }
    }
}
