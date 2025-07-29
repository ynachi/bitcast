use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use tracing::error;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum WorkerSignal {
    Merge,
    Idle,
    Shutdown,
}

struct MergeManager {
    join_handle: JoinHandle<()>,
    trigger: Arc<(Mutex<WorkerSignal>, Condvar)>,
    // TODO: use prom or Otel metrics and add in the metrics crate
    data_file_count: AtomicUsize,
    stale_keys_count: AtomicUsize,
}

impl MergeManager {
    fn new() -> Self {
        let trigger = Arc::new((Mutex::new(WorkerSignal::Idle), Condvar::new()));
        let trigger_clone = trigger.clone();
        let join_handle = std::thread::spawn(move || Self::worker_loop(trigger_clone));
        Self {
            join_handle,
            trigger,
            data_file_count: AtomicUsize::new(0),
            stale_keys_count: AtomicUsize::new(0),
        }
    }

    fn signal(&self, signal: WorkerSignal) {
        let (lock, cvar) = &*self.trigger;
        let mut guard = lock.lock().unwrap_or_else(|_| {
            error!("failed to lock trigger mutex, system exiting");
            std::process::abort();
        });
        *guard = signal;
        cvar.notify_one();
    }

    fn worker_loop(trigger: Arc<(Mutex<WorkerSignal>, Condvar)>) {
        let (lock, cvar) = &*trigger;
        loop {
            let mut guard = lock.lock().unwrap_or_else(|_| {
                error!("failed to lock trigger mutex, system exiting");
                std::process::abort();
            });

            while *guard == WorkerSignal::Idle {
                guard = cvar.wait(guard).unwrap_or_else(|_| {
                    error!("failed to lock trigger mutex, system exiting");
                    std::process::abort();
                });
            }

            match *guard {
                WorkerSignal::Merge => {
                    //TODO Do merge work
                    *guard = WorkerSignal::Idle;
                }
                WorkerSignal::Shutdown => {
                    break;
                }
                WorkerSignal::Idle => {}
            }
        }
    }
}
