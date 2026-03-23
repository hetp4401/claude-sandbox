use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const MAX_LOG_LINES: usize = 500;

/// Thread-safe ring buffer for capturing log lines.
#[derive(Clone)]
pub struct LogBuffer {
    lines: Arc<Mutex<VecDeque<String>>>,
}

impl LogBuffer {
    pub fn new() -> Self {
        Self {
            lines: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_LOG_LINES))),
        }
    }

    pub fn push(&self, line: &str) {
        let mut buf = self.lines.lock().unwrap();
        if buf.len() >= MAX_LOG_LINES {
            buf.pop_front();
        }
        buf.push_back(line.to_string());
    }

    /// Get all lines, optionally only those after a given index.
    /// Returns (lines, next_index).
    pub fn get_since(&self, after: usize) -> (Vec<String>, usize) {
        let buf = self.lines.lock().unwrap();
        let total = buf.len();
        if after >= total {
            return (vec![], total);
        }
        let lines: Vec<String> = buf.iter().skip(after).cloned().collect();
        (lines, total)
    }

    #[cfg(test)]
    pub fn get_all(&self) -> Vec<String> {
        let buf = self.lines.lock().unwrap();
        buf.iter().cloned().collect()
    }
}

/// Macro replacement for println! that also writes to the log buffer.
/// Call `log!(buf, "format {}", args)` instead of `println!(...)`.
#[macro_export]
macro_rules! log {
    ($buf:expr, $($arg:tt)*) => {{
        let msg = format!($($arg)*);
        println!("{}", msg);
        $buf.push(&msg);
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_buffer_new_is_empty() {
        let buf = LogBuffer::new();
        assert!(buf.get_all().is_empty());
    }

    #[test]
    fn test_push_and_get() {
        let buf = LogBuffer::new();
        buf.push("line 1");
        buf.push("line 2");
        let lines = buf.get_all();
        assert_eq!(lines, vec!["line 1", "line 2"]);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let buf = LogBuffer::new();
        for i in 0..600 {
            buf.push(&format!("line {i}"));
        }
        let lines = buf.get_all();
        assert_eq!(lines.len(), MAX_LOG_LINES);
        assert_eq!(lines[0], "line 100");
        assert_eq!(lines[MAX_LOG_LINES - 1], "line 599");
    }

    #[test]
    fn test_get_since() {
        let buf = LogBuffer::new();
        buf.push("a");
        buf.push("b");
        buf.push("c");

        let (lines, next) = buf.get_since(0);
        assert_eq!(lines, vec!["a", "b", "c"]);
        assert_eq!(next, 3);

        let (lines, next) = buf.get_since(1);
        assert_eq!(lines, vec!["b", "c"]);
        assert_eq!(next, 3);

        let (lines, next) = buf.get_since(3);
        assert!(lines.is_empty());
        assert_eq!(next, 3);
    }

    #[test]
    fn test_get_since_after_overflow() {
        let buf = LogBuffer::new();
        buf.push("old");
        let (_, idx) = buf.get_since(0);
        assert_eq!(idx, 1);

        buf.push("new");
        let (lines, idx) = buf.get_since(1);
        assert_eq!(lines, vec!["new"]);
        assert_eq!(idx, 2);
    }

    #[test]
    fn test_clone_shares_state() {
        let buf = LogBuffer::new();
        let buf2 = buf.clone();
        buf.push("from buf1");
        assert_eq!(buf2.get_all(), vec!["from buf1"]);
    }
}
