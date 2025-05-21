//! A notify we can close.
//!
//! In contrast to `Notify` we have (a) dedicated writers to insert events and readers that will
//! consume them. This allows us to notice to close the channel, either explicitly or when all
//! writers are dropped.
use std::sync::{atomic, Arc, Weak};
use tokio::sync::Notify;

pub struct CloseableNotifyWriter {
    inner: Arc<(Notify, atomic::AtomicU64)>,
}

#[derive(Clone)]
pub struct CloseableNotifyReader {
    inner: Weak<(Notify, atomic::AtomicU64)>,
}

impl CloseableNotifyWriter {
    pub fn new() -> (Self, CloseableNotifyReader) {
        let inner = Arc::new((Notify::new(), 1.into()));

        let reader = CloseableNotifyReader {
            inner: Arc::downgrade(&inner),
        };

        (CloseableNotifyWriter { inner }, reader)
    }

    pub fn notify_waiters(&self) {
        self.inner.0.notify_waiters();
    }

    pub fn close(&self) {
        self.inner.1.store(0, atomic::Ordering::Relaxed);
        // Notify waiters after that store.
        self.inner.0.notify_waiters();
    }
}

impl Clone for CloseableNotifyWriter {
    fn clone(&self) -> Self {
        let _ =
            self.inner
                .1
                .fetch_update(atomic::Ordering::Relaxed, atomic::Ordering::Relaxed, |n| {
                    if n > 0 {
                        Some(n + 1)
                    } else {
                        None
                    }
                });

        let inner = self.inner.clone();
        CloseableNotifyWriter { inner }
    }
}

impl Drop for CloseableNotifyWriter {
    fn drop(&mut self) {
        if Ok(1)
            == self.inner.1.fetch_update(
                atomic::Ordering::Relaxed,
                atomic::Ordering::Relaxed,
                |n| if n > 0 { Some(n - 1) } else { None },
            )
        {
            self.close();
        }
    }
}

impl CloseableNotifyReader {
    pub async fn notified(&self) -> Result<(), tokio::sync::TryAcquireError> {
        if let Some(sem) = self.inner.upgrade() {
            let permit = sem.0.notified();

            // Check this after defining a sequence point where we get all notifications that
            // happen afterwards.
            if sem.1.load(atomic::Ordering::Relaxed) == 0 {
                Err(tokio::sync::TryAcquireError::Closed)
            } else {
                permit.await;
                Ok(())
            }
        } else {
            Err(tokio::sync::TryAcquireError::Closed)
        }
    }

    pub fn upgrade(&self) -> Option<CloseableNotifyWriter> {
        let inner = self.inner.upgrade()?;

        loop {
            let old = inner.1.load(atomic::Ordering::Relaxed);

            if old == 0 {
                return None;
            }

            if inner
                .1
                .compare_exchange_weak(
                    old,
                    old + 1,
                    atomic::Ordering::Relaxed,
                    atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        Some(CloseableNotifyWriter { inner })
    }

    pub fn is_closed(&self) -> bool {
        self.inner
            .upgrade()
            .map_or(true, |inner| inner.1.load(atomic::Ordering::Relaxed) == 0)
    }
}
