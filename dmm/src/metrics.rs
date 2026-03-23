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
    pub hashlist_paused: Arc<std::sync::atomic::AtomicBool>,
    pub imdb_paused: Arc<std::sync::atomic::AtomicBool>,
    pub singles_paused: Arc<std::sync::atomic::AtomicBool>,
    pub packs_paused: Arc<std::sync::atomic::AtomicBool>,
    pub dht_paused: Arc<std::sync::atomic::AtomicBool>,
}

impl PipelineControls {
    pub fn new() -> Self {
        Self {
            hashlist_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            imdb_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            singles_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            packs_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            dht_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn is_paused(&self, name: &str) -> bool {
        match name {
            "hashlist" => self.hashlist_paused.load(Ordering::Relaxed),
            "imdb" => self.imdb_paused.load(Ordering::Relaxed),
            "singles" => self.singles_paused.load(Ordering::Relaxed),
            "packs" => self.packs_paused.load(Ordering::Relaxed),
            "dht" => self.dht_paused.load(Ordering::Relaxed),
            _ => false,
        }
    }

    pub fn set_paused(&self, name: &str, paused: bool) {
        match name {
            "hashlist" => self.hashlist_paused.store(paused, Ordering::Relaxed),
            "imdb" => self.imdb_paused.store(paused, Ordering::Relaxed),
            "singles" => self.singles_paused.store(paused, Ordering::Relaxed),
            "packs" => self.packs_paused.store(paused, Ordering::Relaxed),
            "dht" => self.dht_paused.store(paused, Ordering::Relaxed),
            _ => {}
        }
    }

    pub fn status(&self) -> Vec<(&'static str, bool)> {
        vec![
            ("hashlist", !self.is_paused("hashlist")),
            ("imdb", !self.is_paused("imdb")),
            ("singles", !self.is_paused("singles")),
            ("packs", !self.is_paused("packs")),
            ("dht", !self.is_paused("dht")),
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
            "torrents_ingested",
            "hashlists_processed",
            "imdb_resolved",
            "imdb_failed",
            "singles_inserted",
            "packs_resolved",
            "packs_failed",
            "cache_hits",
            "rd_hits",
            "swarm_hits",
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
