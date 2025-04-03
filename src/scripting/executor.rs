use crate::config::RetryStrategy;
use crate::error::LatteError;
use crate::stats::session::SessionStats;
use anyhow::anyhow;
use rand::random;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use try_lock::TryLock;

pub struct Executor<R, E>
where
    R: Sync + Send,
    E: Sync + Send,
{
    stats: TryLock<SessionStats>,
    retry_strategy: RetryStrategy,
    should_retry_fn: Arc<Box<dyn Fn(&Result<R, E>) -> bool>>,
}

impl<R, E> Executor<R, E>
where
    R: Sync + Send,
    E: Sync + Send + Display,
{
    pub fn new<F>(retry_strategy: RetryStrategy, should_retry_fn: F) -> Self
    where
        F: Fn(&Result<R, E>) -> bool + 'static,
    {
        Self {
            stats: TryLock::new(SessionStats::new()),
            retry_strategy,
            should_retry_fn: Arc::new(Box::new(should_retry_fn)),
        }
    }

    pub async fn execute_inner<Res>(
        &self,
        f: impl Fn() -> Res,
        get_rows_fn: impl Fn(&R) -> u64,
    ) -> Result<R, E>
    where
        Res: Future<Output = Result<R, E>>,
    {
        let start_time = self.stats.try_lock().unwrap().start_request();

        let mut rs: Result<R, E> = f().await;
        let mut attempts = 1;
        let retry_strategy = &self.retry_strategy;
        while attempts <= retry_strategy.retries && self.should_retry(&rs, retry_strategy) {
            let current_retry_interval = get_exponential_retry_interval(
                retry_strategy.retry_delay.min,
                retry_strategy.retry_delay.max,
                attempts,
            );
            tokio::time::sleep(current_retry_interval).await;

            rs = f().await;
            attempts += 1;
        }

        let stat_rs = rs
            .as_ref()
            .map(|x| get_rows_fn(x))
            .map_err(|e| anyhow!(format!("{}", e)));

        self.stats.try_lock().unwrap().complete_request(
            start_time.elapsed(),
            &stat_rs,
            attempts - 1,
        );

        rs
    }

    /// Clones the context for use by another thread.
    /// The new clone gets fresh statistics.
    /// The user data gets passed through serialization and deserialization to avoid
    /// accidental data sharing.
    pub fn clone(&self) -> Result<Self, LatteError> {
        Ok(Self {
            stats: TryLock::new(SessionStats::default()),
            retry_strategy: self.retry_strategy,
            should_retry_fn: self.should_retry_fn.clone(),
        })
    }

    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_session_stats(&self) -> SessionStats {
        let mut stats = self.stats.try_lock().unwrap();
        let result = stats.clone();
        stats.reset();
        result
    }

    /// Resets query and request counters
    pub fn reset(&self) {
        self.stats.try_lock().unwrap().reset();
    }

    fn should_retry(&self, result: &Result<R, E>, retry_strategy: &RetryStrategy) -> bool {
        if !result.is_err() {
            return false;
        }
        if retry_strategy.retry_on_all_errors {
            return true;
        }

        (self.should_retry_fn)(result)
    }
}

pub fn get_exponential_retry_interval(
    min_interval: Duration,
    max_interval: Duration,
    current_attempt_num: u64,
) -> Duration {
    let min_interval_float: f64 = min_interval.as_secs_f64();
    let mut current_interval: f64 =
        min_interval_float * (2u64.pow(current_attempt_num.try_into().unwrap_or(0)) as f64);

    // Add jitter
    current_interval += random::<f64>() * min_interval_float;
    current_interval -= min_interval_float / 2.0;

    Duration::from_secs_f64(current_interval.min(max_interval.as_secs_f64()))
}
