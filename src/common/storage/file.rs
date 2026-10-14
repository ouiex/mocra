//! Compatibility name for the single filesystem blob-storage implementation.
//! Stored response messages carry relative keys, independent of the mount location.

pub use crate::utils::storage::FileSystemBlobStorage as FileBlobStorage;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::interface::storage::Offloadable;
    use crate::common::model::{ExecutionMark, Response};
    use crate::utils::storage::BlobStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn stored_key_is_readable_from_another_instance() {
        let root = std::env::temp_dir().join(format!("mocra-blob-{}", uuid::Uuid::now_v7()));
        let writer = FileBlobStorage::new(&root);
        let reader = FileBlobStorage::new(&root);
        let key = writer
            .put("response/run/item.bin", b"payload")
            .await
            .unwrap();
        assert_eq!(key, "response/run/item.bin");
        assert_eq!(reader.get(&key).await.unwrap(), b"payload");
        assert_eq!(
            reader
                .get(&root.join(&key).to_string_lossy())
                .await
                .unwrap(),
            b"payload"
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn response_body_offloads_and_reloads_through_shared_key() {
        let root = std::env::temp_dir().join(format!("mocra-response-{}", uuid::Uuid::now_v7()));
        let writer: Arc<dyn BlobStorage> = Arc::new(FileBlobStorage::new(&root));
        let reader: Arc<dyn BlobStorage> = Arc::new(FileBlobStorage::new(&root));
        let mut response = Response {
            id: uuid::Uuid::now_v7(),
            platform: "site".into(),
            account: "user".into(),
            module: "page".into(),
            status_code: 200,
            cookies: Default::default(),
            content: vec![42; 128],
            storage_path: None,
            headers: vec![],
            task_retry_times: 0,
            metadata: Default::default(),
            download_middleware: vec![],
            data_middleware: vec![],
            task_finished: false,
            context: ExecutionMark::default(),
            run_id: uuid::Uuid::now_v7(),
            prefix_request: uuid::Uuid::nil(),
            request_hash: None,
            priority: Default::default(),
        };
        assert!(response.should_offload(64));
        response.offload(&writer).await.unwrap();
        assert!(response.content.is_empty());
        assert!(
            response
                .storage_path
                .as_ref()
                .unwrap()
                .starts_with("response/")
        );
        response.reload(&reader).await.unwrap();
        assert_eq!(response.content, vec![42; 128]);
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
