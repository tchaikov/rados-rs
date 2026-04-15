//! Per-workload CPU and RSS sampling.
//!
//! CPU times come from `getrusage(RUSAGE_SELF)` deltas (exact, whole-process).
//! RSS is sampled from `/proc/self/status` by a background task; we track the
//! max observed during the run and report both peak and net deltas against
//! the baseline at `start()`.

use serde::Serialize;
use std::fs;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ResourceDelta {
    pub user_cpu: Duration,
    pub sys_cpu: Duration,
    /// Peak RSS observed minus baseline RSS, in bytes. May be negative.
    pub peak_rss_delta: i64,
    /// RSS at stop minus RSS at start, in bytes. May be negative.
    pub net_rss_delta: i64,
}

pub struct ResourceSampler {
    baseline_user_us: i64,
    baseline_sys_us: i64,
    baseline_rss: i64,
    max_rss: Arc<AtomicI64>,
    stop_flag: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl ResourceSampler {
    pub fn start() -> Self {
        let (baseline_user_us, baseline_sys_us) = getrusage_self();
        let baseline_rss = read_rss_bytes();

        let max_rss = Arc::new(AtomicI64::new(baseline_rss));
        let stop_flag = Arc::new(AtomicBool::new(false));

        let max_rss_clone = Arc::clone(&max_rss);
        let stop_flag_clone = Arc::clone(&stop_flag);
        let handle = tokio::spawn(async move {
            while !stop_flag_clone.load(Ordering::Relaxed) {
                let rss = read_rss_bytes();
                max_rss_clone.fetch_max(rss, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        Self {
            baseline_user_us,
            baseline_sys_us,
            baseline_rss,
            max_rss,
            stop_flag,
            handle,
        }
    }

    pub async fn stop(self) -> ResourceDelta {
        self.stop_flag.store(true, Ordering::Relaxed);
        let _ = self.handle.await;

        let final_rss = read_rss_bytes();
        self.max_rss.fetch_max(final_rss, Ordering::Relaxed);
        let peak_rss = self.max_rss.load(Ordering::Relaxed);

        let (user_us, sys_us) = getrusage_self();
        ResourceDelta {
            user_cpu: Duration::from_micros((user_us - self.baseline_user_us) as u64),
            sys_cpu: Duration::from_micros((sys_us - self.baseline_sys_us) as u64),
            peak_rss_delta: peak_rss - self.baseline_rss,
            net_rss_delta: final_rss - self.baseline_rss,
        }
    }
}

fn getrusage_self() -> (i64, i64) {
    unsafe {
        let mut usage = MaybeUninit::<libc::rusage>::uninit();
        let rc = libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        assert_eq!(rc, 0, "getrusage failed");
        let usage = usage.assume_init();
        // `time_t` and `suseconds_t` are `i64` on Linux x86_64 but can be
        // narrower (`i32`) on 32-bit platforms; coerce via `as i64` so the
        // arithmetic is portable. Clippy flags the redundancy on hosts
        // where both are already `i64`, so silence the lint locally.
        #[allow(clippy::unnecessary_cast)]
        let user = (usage.ru_utime.tv_sec as i64) * 1_000_000 + usage.ru_utime.tv_usec as i64;
        #[allow(clippy::unnecessary_cast)]
        let sys = (usage.ru_stime.tv_sec as i64) * 1_000_000 + usage.ru_stime.tv_usec as i64;
        (user, sys)
    }
}

/// Parse VmRSS from /proc/self/status. Returns bytes, or 0 on failure.
fn read_rss_bytes() -> i64 {
    let Ok(contents) = fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let rest = rest.trim();
            let kb: i64 = rest
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            return kb * 1024;
        }
    }
    0
}
