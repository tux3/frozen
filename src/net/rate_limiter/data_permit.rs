use crossbeam::queue::ArrayQueue;
use futures_intrusive::sync::SemaphoreReleaser;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

pub struct RateLimitPermit<'rate_limiter, T: Debug> {
    _releaser: SemaphoreReleaser<'rate_limiter>,
    data_queue: &'rate_limiter ArrayQueue<Option<T>>,
    data: Option<T>,
}

impl<'r, T: Debug> RateLimitPermit<'r, T> {
    pub fn new(releaser: SemaphoreReleaser<'r>, data_queue: &'r ArrayQueue<Option<T>>) -> Self {
        let data = data_queue
            .pop()
            .expect("The data queue should be behind a semaphore and never underflow");
        Self {
            _releaser: releaser,
            data_queue,
            data,
        }
    }
}

impl<T: Debug> Deref for RateLimitPermit<'_, T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: Debug> DerefMut for RateLimitPermit<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T: Debug> Drop for RateLimitPermit<'_, T> {
    fn drop(&mut self) {
        self.data_queue
            .push(self.data.take())
            .expect("The bounded data queue should never overflow");
    }
}
