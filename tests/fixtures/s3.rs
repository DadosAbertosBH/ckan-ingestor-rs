use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::Client;
use ckan_ingestor_rs::config::S3Settings;
use futures::executor;
use rstest::fixture;
use testcontainers::{
    core::IntoContainerPort, runners::SyncRunner, Container, GenericImage, ImageExt,
};

struct MinioContainer {
    minio: Container<GenericImage>,
    address: String,
}

#[fixture]
#[once]
pub fn minio() -> MinioContainer {
    let minio = GenericImage::new(
        "minio/minio",
        "RELEASE.2024-09-22T00-33-43Z", /* &str */
    )
    .with_exposed_port(9000.tcp())
    .with_exposed_port(9001.tcp())
    .with_env_var("MINIO_ACCESS_KEY", "minioadmin")
    .with_env_var("MINIO_SECRET_KEY", "minioadmin")
    .with_cmd(vec![
        "server".to_string(),
        "/data".to_string(),
        "--address".to_string(),
        ":9000".to_string(),
        "--console-address".to_string(),
        ":9001".to_string(),
    ]);

    executor::block_on(async {
        tokio::task::spawn_blocking(|| {
            let container = minio.start().expect("Can't started minio.");
            let port = container
                .get_host_port_ipv4(9000)
                .expect("Failed to get host port for MinIO");
            MinioContainer {
                minio: container,
                address: format!("127.0.0.1:{}", port),
            }
        })
        .await
        .expect("Failed to start minio")
    })
}

#[fixture]
pub async fn s3_client(minio: &MinioContainer) -> Client {
    //noinspection HttpUrlsUsage
    let endpoint_uri = format!("http://{}", minio.address);
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "test");

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(endpoint_uri)
        .credentials_provider(creds)
        .load()
        .await;

    Client::new(&sdk_config)
}

pub async fn setup_minio_bucket_and_policy(client: &Client) {
    let bucket = "warehouse";
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload"
                ],
                "Resource": format!("arn:aws:s3:::{}{}", bucket, "/docs/*"),
            }
        ]
    });
    let _ = client.create_bucket().bucket(bucket).send().await.unwrap();
    let result = client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy.to_string())
        .send()
        .await
        .unwrap();
    assert!(result.request_id().is_some())
}

#[fixture]
pub async fn s3_settings(#[future] s3_client: Client, minio: &MinioContainer) -> S3Settings {
    let s3_client = s3_client.await;
    setup_minio_bucket_and_policy(&s3_client).await;
    S3Settings {
        endpoint: minio.address.clone(),
        bucket: "warehouse".into(),
        use_ssl: false,
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".to_string(),
        ..Default::default()
    }
}
