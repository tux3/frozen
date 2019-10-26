use tokio_executor::threadpool;
use tokio_net::driver::{self, Reactor};
use tokio_timer::{timer, Timer, clock, clock::Clock};
use std::future::Future;
use crate::box_result::BoxResult;
use std::sync::Mutex;

/// This runtime is meant to be used in a local scope of an async fn
/// Essentially a simplified copy of tokio's Runtime where shutdown functions can be awaited
pub struct ScopedRuntime {
    executor: threadpool::ThreadPool,
}

pub struct Builder {
    core_threads: usize,
    threadpool_builder: threadpool::Builder,
}

impl Builder {
    pub fn new() -> Self {
        let core_threads = num_cpus::get().max(1);
        let mut threadpool_builder = threadpool::Builder::new();
        threadpool_builder.pool_size(core_threads);

        Self {
            core_threads,
            threadpool_builder,
        }
    }

    pub fn pool_size(&mut self, val: usize) -> &mut Self {
        self.core_threads = val;
        self.threadpool_builder.pool_size(val);
        self
    }

    pub fn name_prefix<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self.threadpool_builder.name_prefix(val);
        self
    }

    pub fn build(&mut self) -> BoxResult<ScopedRuntime> {
        let clock = Clock::new();
        let mut reactor_handles = Vec::new();
        let mut timer_handles = Vec::new();
        let mut timers = Vec::new();
        for _ in 0..self.core_threads {
            // Create a new reactor.
            let reactor = Reactor::new()?;
            reactor_handles.push(reactor.handle());

            // Create a new timer.
            let timer = Timer::new_with_now(reactor, clock.clone());
            timer_handles.push(timer.handle());
            timers.push(Mutex::new(Some(timer)));
        }

        let executor = self
            .threadpool_builder
            .around_worker(move |w| {
                let index = w.id().to_usize();
                let _reactor_guard = driver::set_default(&reactor_handles[index]);

                clock::with_default(&clock, || {
                    let _timer_guard = timer::set_default(&timer_handles[index]);
                    w.run();
                })
            })
            .custom_park(move |worker_id| {
                let index = worker_id.to_usize();

                timers[index]
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
            })
            .build();

        Ok(ScopedRuntime {
            executor,
        })
    }
}

impl ScopedRuntime {
    pub fn spawn<F>(&self, future: F)
        where F: Future<Output = ()> + Send + 'static,
    {
        self.executor.spawn(future)
    }

    pub fn shutdown_on_idle(self) -> threadpool::Shutdown {
        self.executor.shutdown_on_idle()
    }
}