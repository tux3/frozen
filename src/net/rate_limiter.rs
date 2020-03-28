pub use self::data_permit::RateLimitPermit;
use crate::config::Config;
use crate::net::b2::{B2Upload, B2};
use crossbeam::queue::ArrayQueue;
use futures_intrusive::sync::{Semaphore, SemaphoreReleaser};

mod data_permit;

pub struct RateLimiter {
    b2: B2,

    download_sem: Semaphore,
    delete_sem: Semaphore,
    upload_sem: Semaphore,

    upload_urls: ArrayQueue<Option<B2Upload>>,
}

impl RateLimiter {
    pub fn new(config: &Config, b2: &B2) -> Self {
        let upload_urls = ArrayQueue::new(config.upload_threads as usize);
        for _ in 0..config.upload_threads {
            upload_urls.push(None).unwrap();
        }

        Self {
            b2: b2.clone(),
            upload_sem: Semaphore::new(false, config.upload_threads as usize),
            download_sem: Semaphore::new(false, config.download_threads as usize),
            delete_sem: Semaphore::new(false, config.delete_threads as usize),
            upload_urls,
        }
    }

    pub fn b2_client(&self) -> &B2 {
        &self.b2
    }

    pub async fn borrow_upload_permit(&self) -> RateLimitPermit<'_, B2Upload> {
        let releaser = self.upload_sem.acquire(1).await;
        RateLimitPermit::new(releaser, &self.upload_urls)
    }

    pub async fn borrow_download_permit(&self) -> SemaphoreReleaser<'_> {
        self.download_sem.acquire(1).await
    }

    pub async fn borrow_delete_permit(&self) -> SemaphoreReleaser<'_> {
        self.delete_sem.acquire(1).await
    }
}
