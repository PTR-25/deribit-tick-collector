use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::{mpsc, watch};
use tracing::{debug, info};

use crate::config::{S3Config, StorageConfig};
use crate::error::Result;
use crate::telemetry::Telemetry;

#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "s3")]
use object_store::path::Path as ObjPath;
#[cfg(feature = "s3")]
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
#[cfg(feature = "s3")]
use std::sync::atomic::Ordering;
#[cfg(feature = "s3")]
use tokio::sync::Semaphore;
#[cfg(feature = "s3")]
use tracing::{error, warn};

pub type UploaderHandle = (
    Option<mpsc::Sender<PathBuf>>,
    Option<tokio::task::JoinHandle<()>>,
);

/// Spawn the asynchronous S3 / S3-compatible uploader task (no-op if `s3.enabled = false`).
pub fn spawn_uploader(
    s3_cfg: S3Config,
    base_data_dir: PathBuf,
    telemetry: Arc<Telemetry>,
) -> Result<UploaderHandle> {
    if !s3_cfg.enabled {
        return Ok((None, None));
    }

    #[cfg(not(feature = "s3"))]
    {
        let _ = (base_data_dir, telemetry);
        Err(crate::error::CollectorError::Config(
            "s3.enabled = true but binary was compiled without the `s3` feature".into(),
        ))
    }

    #[cfg(feature = "s3")]
    {
        let bucket = s3_cfg.bucket.clone().ok_or_else(|| {
            crate::error::CollectorError::Config("s3.bucket must be configured".into())
        })?;

        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .with_region(&s3_cfg.region);

        if let Some(ep) = s3_cfg.endpoint.as_deref().filter(|s| !s.trim().is_empty()) {
            builder = builder
                .with_endpoint(ep)
                .with_allow_http(ep.starts_with("http://"));
        }

        let store: Arc<dyn ObjectStore> = Arc::new(builder.build()?);
        let (tx, mut rx) = mpsc::channel::<PathBuf>(4096);
        let sem = Arc::new(Semaphore::new(s3_cfg.max_concurrent_uploads.max(1)));
        let prefix = s3_cfg.prefix.trim_matches('/').to_string();
        let max_retries = s3_cfg.max_retries.max(1);
        let delete_local = s3_cfg.delete_local_after_upload;

        info!(
            bucket = %bucket,
            region = %s3_cfg.region,
            prefix = %prefix,
            delete_local,
            "initialized async S3 uploader"
        );

        let handle = tokio::spawn(async move {
            let mut in_flight = Vec::new();
            while let Some(local_path) = rx.recv().await {
                let Ok(permit) = Arc::clone(&sem).acquire_owned().await else {
                    break;
                };
                let store = Arc::clone(&store);
                let base = base_data_dir.clone();
                let prefix = prefix.clone();
                let telemetry = Arc::clone(&telemetry);

                let task = tokio::spawn(async move {
                    let _permit = permit;
                    upload_single_file(
                        store,
                        &local_path,
                        &base,
                        &prefix,
                        max_retries,
                        delete_local,
                        &telemetry,
                    )
                    .await;
                });
                in_flight.push(task);
                in_flight.retain(|h| !h.is_finished());
            }
            for h in in_flight {
                let _ = h.await;
            }
            info!("S3 uploader drained all pending uploads");
        });

        Ok((Some(tx), Some(handle)))
    }
}

#[cfg(feature = "s3")]
async fn upload_single_file(
    store: Arc<dyn ObjectStore>,
    local_path: &Path,
    base_dir: &Path,
    prefix: &str,
    max_retries: usize,
    delete_local: bool,
    telemetry: &Telemetry,
) {
    let rel = local_path
        .strip_prefix(base_dir)
        .unwrap_or(local_path)
        .to_string_lossy()
        .replace('\\', "/");
    let key = if prefix.is_empty() {
        rel
    } else {
        format!("{prefix}/{rel}")
    };
    let obj_path = ObjPath::from(key.as_str());

    let bytes = match tokio::fs::read(local_path).await {
        Ok(b) => b,
        Err(e) => {
            error!(path = %local_path.display(), error = %e, "failed reading local file for S3 upload");
            telemetry.s3_uploads_err.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let size = bytes.len() as u64;
    let payload = PutPayload::from(bytes);

    for attempt in 0..max_retries {
        match store.put(&obj_path, payload.clone()).await {
            Ok(_) => {
                telemetry.s3_uploads_ok.fetch_add(1, Ordering::Relaxed);
                telemetry
                    .s3_bytes_uploaded
                    .fetch_add(size, Ordering::Relaxed);
                debug!(key = %key, bytes = size, "uploaded Parquet segment to S3");
                if delete_local {
                    if let Err(e) = tokio::fs::remove_file(local_path).await {
                        warn!(path = %local_path.display(), error = %e, "failed deleting uploaded local segment");
                    } else {
                        prune_empty_parents(local_path, base_dir);
                    }
                }
                return;
            }
            Err(e) => {
                let is_last = attempt + 1 >= max_retries;
                if is_last {
                    error!(key = %key, error = %e, "S3 upload failed after all retries");
                    telemetry.s3_uploads_err.fetch_add(1, Ordering::Relaxed);
                } else {
                    let backoff = Duration::from_secs(1u64 << attempt.min(5));
                    warn!(key = %key, attempt = attempt + 1, error = %e, "S3 upload failed; retrying");
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

/// Spawn periodic local retention pruner when `storage.local_retention_hours > 0`.
pub fn spawn_retention_pruner(
    cfg: StorageConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    if cfg.local_retention_hours == 0 {
        return None;
    }
    let max_age = Duration::from_secs(cfg.local_retention_hours * 3600);
    let base_dir = cfg.data_dir;

    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1800));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = tick.tick() => {
                    let base = base_dir.clone();
                    let removed = tokio::task::spawn_blocking(move || prune_old_parquet(&base, max_age))
                        .await
                        .unwrap_or(0);
                    if removed > 0 {
                        info!(removed, "pruned expired local Parquet files");
                    }
                }
            }
        }
    }))
}

fn prune_old_parquet(base_dir: &Path, max_age: Duration) -> usize {
    let now = SystemTime::now();
    let mut removed = 0usize;
    let mut stack = vec![base_dir.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|s| s.to_str()) == Some("parquet")
                && let Ok(meta) = entry.metadata()
                && let Ok(modified) = meta.modified()
                && let Ok(age) = now.duration_since(modified)
                && age > max_age
                && fs::remove_file(&path).is_ok()
            {
                removed += 1;
                prune_empty_parents(&path, base_dir);
            }
        }
    }
    removed
}

fn prune_empty_parents(file_path: &Path, stop_at: &Path) {
    let mut cur = file_path.parent();
    while let Some(dir) = cur {
        if dir == stop_at {
            break;
        }
        let is_empty = fs::read_dir(dir)
            .map(|mut it| it.next().is_none())
            .unwrap_or(false);
        if is_empty {
            let _ = fs::remove_dir(dir);
            cur = dir.parent();
        } else {
            break;
        }
    }
    debug!("pruned empty parent dirs up to {}", stop_at.display());
}
