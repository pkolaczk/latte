use console::style;
use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::time::{Duration, Instant};

enum ProgressBound {
    Duration(Duration),
    Count(u64),
}

pub struct Progress {
    start_time: Instant,
    bound: ProgressBound,
    pos: AtomicU64,
    msg: String,
}

impl Progress {
    pub fn with_duration(msg: String, max_time: Duration) -> Progress {
        Progress {
            start_time: Instant::now(),
            bound: ProgressBound::Duration(max_time),
            pos: AtomicU64::new(0),
            msg,
        }
    }

    pub fn with_count(msg: String, count: u64) -> Progress {
        Progress {
            start_time: Instant::now(),
            bound: ProgressBound::Count(count),
            pos: AtomicU64::new(0),
            msg,
        }
    }

    pub fn tick(&self) {
        self.pos.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns progress bar as string `[====>   ]`
    fn bar(fill_len: usize, total_len: usize) -> String {
        let fill_len = min(fill_len, total_len);
        format!(
            "[{}{}]",
            "▪".repeat(fill_len),
            " ".repeat(total_len - fill_len)
        )
    }
}

impl Display for Progress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        const WIDTH: usize = 60;
        let pos = self.pos.load(Ordering::Relaxed);
        let text = match self.bound {
            ProgressBound::Count(count) => {
                let fill = (WIDTH as u64 * pos / count) as usize;
                format!(
                    "{} {:>5.1}%      {:>28}",
                    Self::bar(fill, WIDTH),
                    100.0 * pos as f32 / count as f32,
                    format!("{}/{}", pos, count)
                )
            }
            ProgressBound::Duration(duration) => {
                let elapsed_secs = (Instant::now() - self.start_time).as_secs_f32();
                let duration_secs = duration.as_secs_f32();
                let fill = (WIDTH as f32 * elapsed_secs / duration_secs) as usize;
                format!(
                    "{} {:>5.1}% {:>20} {:>12}",
                    Self::bar(fill, WIDTH),
                    100.0 * elapsed_secs / duration_secs,
                    format!("{:.1}/{:.0}s", elapsed_secs, duration_secs),
                    pos
                )
            }
        };

        write!(
            f,
            "\n{:21}{}",
            style(&self.msg)
                .white()
                .bright()
                .bold()
                .on_color256(59)
                .for_stderr(),
            style(text).white().bright().on_color256(59).for_stderr()
        )
    }
}
