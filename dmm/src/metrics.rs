use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

const RING_SIZE: usize = 60; // 60 snapshots = 60 minutes of history at 60s intervals

/// A single named counter.
struct Counter {
    value: AtomicU64,
}

impl Counter {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    fn bump(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    fn take(&self) -> u64 {
        self.value.swap(0, Ordering::Relaxed)
    }
}

/// A ring buffer of snapshots for one counter.
#[derive(Clone, Serialize)]
pub struct CounterHistory {
    pub name: String,
    pub points: Vec<u64>,
    pub total: u64,
    pub last: u64,
}

/// Pause state for each pipeline.
#[derive(Clone)]
pub struct PipelineControls {
    paused: Arc<std::sync::RwLock<std::collections::HashMap<String, std::sync::atomic::AtomicBool>>>,
}

impl PipelineControls {
    pub fn new() -> Self {
        let mut map = std::collections::HashMap::new();
        for name in &["hashlists", "extract", "parse", "imdb", "singles", "packs"] {
            map.insert(name.to_string(), std::sync::atomic::AtomicBool::new(false));
        }
        Self { paused: Arc::new(std::sync::RwLock::new(map)) }
    }

    pub fn is_paused(&self, name: &str) -> bool {
        self.paused.read().unwrap().get(name).map(|v| v.load(Ordering::Relaxed)).unwrap_or(false)
    }

    pub fn set_paused(&self, name: &str, paused: bool) {
        if let Some(v) = self.paused.read().unwrap().get(name) {
            v.store(paused, Ordering::Relaxed);
        }
    }

    pub fn status(&self) -> Vec<(&'static str, bool)> {
        vec![
            ("hashlists", !self.is_paused("hashlists")),
            ("extract", !self.is_paused("extract")),
            ("parse", !self.is_paused("parse")),
            ("imdb", !self.is_paused("imdb")),
            ("singles", !self.is_paused("singles")),
            ("packs", !self.is_paused("packs")),
        ]
    }
}

/// All metrics for the application.
#[derive(Clone)]
pub struct Metrics {
    counters: Arc<Vec<(String, Arc<Counter>)>>,
    history: Arc<RwLock<Vec<(String, Vec<u64>, u64)>>>, // (name, ring, total)
    pub controls: PipelineControls,
}

impl Metrics {
    pub fn new() -> Self {
        let names = vec![
            "hashlists_discovered",
            "hashlists_extracted",
            "torrents_ingested",
            "torrents_parsed",
            "imdb_resolved",
            "imdb_failed",
            "imdb_cache_hits",
            "singles_inserted",
            "packs_resolved",
            "packs_missed",
            "cache_hits",
            "rd_hits",
            "api_calls",
            "db_queries",
        ];

        let counters: Vec<(String, Arc<Counter>)> = names
            .iter()
            .map(|n| (n.to_string(), Arc::new(Counter::new())))
            .collect();

        let history: Vec<(String, Vec<u64>, u64)> = names
            .iter()
            .map(|n| (n.to_string(), vec![0; RING_SIZE], 0))
            .collect();

        Self {
            counters: Arc::new(counters),
            history: Arc::new(RwLock::new(history)),
            controls: PipelineControls::new(),
        }
    }

    /// Bump a counter by n.
    pub fn bump(&self, name: &str, n: u64) {
        for (cname, counter) in self.counters.iter() {
            if cname == name {
                counter.bump(n);
                return;
            }
        }
    }

    /// Snapshot all counters into the ring buffer. Call every 60s.
    pub async fn snapshot(&self) {
        let mut history = self.history.write().await;
        for (i, (_, counter)) in self.counters.iter().enumerate() {
            let val = counter.take();
            if let Some((_, ring, total)) = history.get_mut(i) {
                ring.push(val);
                if ring.len() > RING_SIZE {
                    ring.remove(0);
                }
                *total += val;
            }
        }
    }

    /// Get all counter histories for the API.
    pub async fn get_all(&self) -> Vec<CounterHistory> {
        let history = self.history.read().await;
        history
            .iter()
            .map(|(name, ring, total)| CounterHistory {
                name: name.clone(),
                points: ring.clone(),
                total: *total,
                last: *ring.last().unwrap_or(&0),
            })
            .collect()
    }

    /// Start the background snapshot task.
    pub fn start_snapshot_task(self) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                self.snapshot().await;
            }
        });
    }
}
