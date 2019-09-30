use std::ops::{Deref, DerefMut};
use futures_intrusive::sync::SemaphoreReleaser;
use crossbeam::queue::ArrayQueue;

pub struct DataPermit<'rate_limiter, T> {
    releaser: SemaphoreReleaser<'rate_limiter>,
    data_queue: &'rate_limiter ArrayQueue<Option<T>>,
    data: Option<T>,
}

impl<'r, T> DataPermit<'r, T> {
    pub fn new(releaser: SemaphoreReleaser<'r>, data_queue: &'r ArrayQueue<Option<T>>) -> Self {
        let data = data_queue.pop().expect("The data queue should be behind a semaphore and never underflow");
        Self {
            releaser,
            data_queue,
            data,
        }
    }
}

impl<T> Deref for DataPermit<'_, T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for DataPermit<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> Drop for DataPermit<'_, T> {
    fn drop(&mut self) {
        self.data_queue.push(self.data.take()).expect("The bounded data queue should never overflow");
    }
}