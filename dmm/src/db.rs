use serde::Serialize;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

/// Per-pipeline tuning: batch size, concurrency, max attempts.
/// All configurable via env vars with sensible defaults.
#[derive(Clone)]
pub struct PipelineTuning {
    pub batch_size: i64,
    pub concurrency: usize,
    pub max_attempts: i32,
    pub retry_cooldown_mins: i64,
}

impl PipelineTuning {
    fn from_env(prefix: &str, default_batch: i64, default_concurrency: usize, default_max_attempts: i32, default_cooldown_mins: i64) -> Self {
        fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
        }
        Self {
            batch_size: env_or(&format!("{prefix}_BATCH_SIZE"), default_batch),
            concurrency: env_or(&format!("{prefix}_CONCURRENCY"), default_concurrency),
            max_attempts: env_or(&format!("{prefix}_MAX_ATTEMPTS"), default_max_attempts),
            retry_cooldown_mins: env_or(&format!("{prefix}_RETRY_COOLDOWN_MINS"), default_cooldown_mins),
        }
    }
}

#[derive(Clone)]
pub struct PipelineConfig {
    pub extract: PipelineTuning,
    pub parse: PipelineTuning,
    pub imdb: PipelineTuning,
    pub singles: PipelineTuning,
    pub packs: PipelineTuning,
}

impl PipelineConfig {
    pub fn from_env() -> Self {
        Self {
            extract:  PipelineTuning::from_env("EXTRACT",  50,   8,     10000, 20),
            parse:    PipelineTuning::from_env("PARSE",    5000, 1,     10000, 20),
            imdb:     PipelineTuning::from_env("IMDB",     2000, 30,    10000, 20),
            singles:  PipelineTuning::from_env("SINGLES",  5000, 1,     10000, 20),
            packs:    PipelineTuning::from_env("PACKS",    200,  20,    10000, 20),
        }
    }
}

#[derive(Clone)]
pub struct Db {
    client: Arc<Client>,
    pub config: PipelineConfig,
}

// =========================================================================
// Row types
// =========================================================================

#[derive(Debug, Clone, Serialize)]
pub struct TorrentRow {
    pub hash: String,
    pub filename: String,
    pub size_bytes: i64,
    pub completed: bool,
    pub attempts: i32,
    pub last_processed: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParsedTorrentRow {
    pub hash: String,
    pub title: String,
    pub year: Option<i32>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    pub completed: bool,
    pub attempts: i32,
    pub last_processed: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImdbMappingRow {
    pub hash: String,
    pub imdb_id: String,
    pub content_type: String,
    pub completed: bool,
    pub attempts: i32,
    pub last_processed: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamRow {
    pub torrent_hash: String,
    pub imdb_id: String,
    pub stream_type: String,
    pub size_bytes: i64,
    pub file_index: Option<i32>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    pub filename: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Paginated<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineStats {
    pub unresolved: i64,
    pub completed: i64,
    pub exhausted: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueDepths {
    pub hashlists: PipelineStats,
    pub extract: PipelineStats,
    pub parse: PipelineStats,
    pub imdb: PipelineStats,
    pub singles: PipelineStats,
    pub packs: PipelineStats,
}

pub struct RecordCounts {
    pub hashlists: i64,
    pub torrents: i64,
    pub parsed: i64,
    pub imdb: i64,
    pub streams: i64,
}

impl Db {
    pub async fn connect(database_url: &str) -> Result<Self, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {e}");
            }
        });
        let db = Self {
            client: Arc::new(client),
            config: PipelineConfig::from_env(),
        };
        db.init_schema().await?;
        Ok(db)
    }

    async fn init_schema(&self) -> Result<(), tokio_postgres::Error> {
        self.client
            .batch_execute(
                "
            -- Pipeline 1: hashlist names discovered from GitHub
            CREATE TABLE IF NOT EXISTS dmm_hashlists (
                name TEXT PRIMARY KEY,
                completed BOOLEAN NOT NULL DEFAULT FALSE,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_processed TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Pipeline 2: raw torrents extracted from hashlists
            CREATE TABLE IF NOT EXISTS torrents (
                hash TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                completed BOOLEAN NOT NULL DEFAULT FALSE,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_processed TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Pipeline 3: parsed metadata from torrent filenames
            CREATE TABLE IF NOT EXISTS parsed_torrents (
                hash TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                year INTEGER,
                season INTEGER,
                episode INTEGER,
                completed BOOLEAN NOT NULL DEFAULT FALSE,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_processed TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Pipeline 4: IMDb ID mappings
            CREATE TABLE IF NOT EXISTS imdb_mappings (
                hash TEXT PRIMARY KEY,
                imdb_id TEXT NOT NULL,
                content_type TEXT NOT NULL DEFAULT 'movie',
                completed BOOLEAN NOT NULL DEFAULT FALSE,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_processed TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Pipeline 5+6 output: final streams
            CREATE TABLE IF NOT EXISTS streams (
                id SERIAL PRIMARY KEY,
                torrent_hash TEXT NOT NULL,
                imdb_id TEXT NOT NULL,
                stream_type TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                file_index INTEGER,
                season INTEGER,
                episode INTEGER,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(torrent_hash, file_index)
            );

            CREATE INDEX IF NOT EXISTS idx_dmm_hashlists_pending ON dmm_hashlists(last_processed ASC) WHERE completed = false;
            CREATE INDEX IF NOT EXISTS idx_torrents_pending ON torrents(last_processed ASC) WHERE completed = false;
            CREATE INDEX IF NOT EXISTS idx_parsed_torrents_pending ON parsed_torrents(last_processed ASC) WHERE completed = false;
            CREATE INDEX IF NOT EXISTS idx_imdb_mappings_pending ON imdb_mappings(last_processed ASC) WHERE completed = false;
            CREATE INDEX IF NOT EXISTS idx_streams_torrent_hash ON streams(torrent_hash);
            CREATE INDEX IF NOT EXISTS idx_streams_imdb_id ON streams(imdb_id);
            ",
            )
            .await?;
        Ok(())
    }

    // =========================================================================
    // Pipeline 1: GitHub → dmm_hashlists
    // =========================================================================

    /// Insert a new hashlist name. Does nothing if it already exists.
    pub async fn insert_hashlist_name(&self, name: &str) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO dmm_hashlists (name) VALUES ($1) ON CONFLICT DO NOTHING",
                &[&name],
            )
            .await?;
        Ok(())
    }

    // =========================================================================
    // Pipeline 2: dmm_hashlists → torrents
    // =========================================================================

    pub async fn get_pending_hashlists(&self, limit: i64) -> Result<Vec<String>, tokio_postgres::Error> {
        let max = self.config.extract.max_attempts;
        let cooldown = self.config.extract.retry_cooldown_mins;
        let rows = self.client.query(
            &format!("SELECT name FROM dmm_hashlists WHERE completed = false AND attempts < $1 AND (attempts = 0 OR last_processed < NOW() - INTERVAL '{cooldown} minutes') ORDER BY last_processed ASC LIMIT $2"),
            &[&max, &limit],
        ).await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    /// Touch before processing: bump attempts + update timestamp.
    pub async fn touch_hashlist(&self, name: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE dmm_hashlists SET attempts = attempts + 1, last_processed = NOW() WHERE name = $1",
            &[&name],
        ).await?;
        Ok(())
    }

    pub async fn complete_hashlist(&self, name: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE dmm_hashlists SET completed = true, last_processed = NOW() WHERE name = $1",
            &[&name],
        ).await?;
        Ok(())
    }

    /// Upsert a torrent. If new, sets completed=false and last_processed=NOW().
    pub async fn upsert_torrent(
        &self,
        hash: &str,
        filename: &str,
        size_bytes: i64,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO torrents (hash, filename, size_bytes)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (hash) DO UPDATE SET filename = $2, size_bytes = $3",
                &[&hash, &filename, &size_bytes],
            )
            .await?;
        Ok(())
    }

    // =========================================================================
    // Pipeline 3: torrents → parsed_torrents
    // =========================================================================

    pub async fn get_pending_torrents(&self, limit: i64) -> Result<Vec<(String, String)>, tokio_postgres::Error> {
        let max = self.config.parse.max_attempts;
        let cooldown = self.config.parse.retry_cooldown_mins;
        let rows = self.client.query(
            &format!("SELECT hash, filename FROM torrents WHERE completed = false AND attempts < $1 AND (attempts = 0 OR last_processed < NOW() - INTERVAL '{cooldown} minutes') ORDER BY last_processed ASC LIMIT $2"),
            &[&max, &limit],
        ).await?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    pub async fn touch_torrent(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE torrents SET attempts = attempts + 1, last_processed = NOW() WHERE hash = $1", &[&hash],
        ).await?;
        Ok(())
    }

    pub async fn complete_torrent(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE torrents SET completed = true, last_processed = NOW() WHERE hash = $1", &[&hash],
        ).await?;
        Ok(())
    }

    /// Insert parsed torrent metadata.
    pub async fn upsert_parsed_torrent(
        &self,
        hash: &str,
        title: &str,
        year: Option<i32>,
        season: Option<i32>,
        episode: Option<i32>,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO parsed_torrents (hash, title, year, season, episode)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (hash) DO UPDATE
                 SET title = $2, year = $3, season = $4, episode = $5",
                &[&hash, &title, &year, &season, &episode],
            )
            .await?;
        Ok(())
    }

    // =========================================================================
    // Pipeline 4: parsed_torrents → imdb_mappings
    // =========================================================================

    pub async fn get_pending_parsed_torrents(
        &self,
        limit: i64,
    ) -> Result<Vec<ParsedTorrentRow>, tokio_postgres::Error> {
        let max = self.config.imdb.max_attempts;
        let cooldown = self.config.imdb.retry_cooldown_mins;
        let rows = self
            .client
            .query(
                &format!("SELECT p.hash, p.title, p.year, p.season, p.episode, p.last_processed
                 FROM parsed_torrents p
                 WHERE p.completed = false AND p.attempts < $1
                   AND (p.attempts = 0 OR p.last_processed < NOW() - INTERVAL '{cooldown} minutes')
                 ORDER BY p.last_processed ASC
                 LIMIT $2"),
                &[&max, &limit],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let ts: chrono::DateTime<chrono::Utc> = r.get(5);
                ParsedTorrentRow {
                    hash: r.get(0),
                    title: r.get(1),
                    year: r.get(2),
                    season: r.get(3),
                    episode: r.get(4),
                    completed: false,
                    attempts: 0,
                    last_processed: ts.to_rfc3339(),
                }
            })
            .collect())
    }

    pub async fn touch_parsed_torrent(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE parsed_torrents SET attempts = attempts + 1, last_processed = NOW() WHERE hash = $1", &[&hash],
        ).await?;
        Ok(())
    }

    pub async fn complete_parsed_torrent(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE parsed_torrents SET completed = true, last_processed = NOW() WHERE hash = $1", &[&hash],
        ).await?;
        Ok(())
    }

    pub async fn upsert_imdb_mapping(
        &self,
        hash: &str,
        imdb_id: &str,
        content_type: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO imdb_mappings (hash, imdb_id, content_type)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (hash) DO UPDATE SET imdb_id = $2, content_type = $3",
                &[&hash, &imdb_id, &content_type],
            )
            .await?;
        Ok(())
    }

    /// IMDb resolution failed — bump attempts + timestamp on parsed_torrents.
    pub async fn mark_imdb_failure(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE parsed_torrents SET attempts = attempts + 1, last_processed = NOW() WHERE hash = $1",
            &[&hash],
        ).await?;
        Ok(())
    }

    // =========================================================================
    // Pipeline 5: imdb_mappings (singles) → streams
    // =========================================================================

    pub async fn get_pending_singles(
        &self,
        limit: i64,
    ) -> Result<Vec<(String, String, String, i64, Option<i32>, Option<i32>)>, tokio_postgres::Error> {
        let max = self.config.singles.max_attempts;
        let cooldown = self.config.singles.retry_cooldown_mins;
        let rows = self.client.query(
            &format!("SELECT m.hash, m.imdb_id, m.content_type, t.size_bytes, p.season, p.episode
             FROM imdb_mappings m
             JOIN torrents t ON m.hash = t.hash
             JOIN parsed_torrents p ON m.hash = p.hash
             WHERE m.completed = false AND m.attempts < $1
               AND (m.attempts = 0 OR m.last_processed < NOW() - INTERVAL '{cooldown} minutes')
               AND (m.content_type = 'movie'
                    OR (m.content_type = 'series' AND p.season IS NOT NULL AND p.episode IS NOT NULL))
             ORDER BY m.last_processed ASC
             LIMIT $2"),
            &[&max, &limit],
        ).await?;
        Ok(rows
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5)))
            .collect())
    }

    pub async fn touch_imdb_mapping(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE imdb_mappings SET attempts = attempts + 1, last_processed = NOW() WHERE hash = $1", &[&hash],
        ).await?;
        Ok(())
    }

    pub async fn complete_imdb_mapping(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client.execute(
            "UPDATE imdb_mappings SET completed = true, last_processed = NOW() WHERE hash = $1", &[&hash],
        ).await?;
        Ok(())
    }

    // =========================================================================
    // Pipeline 6: imdb_mappings (packs) → streams
    // =========================================================================

    pub async fn get_pending_packs(
        &self,
        limit: i64,
    ) -> Result<Vec<(String, String, i32, i64)>, tokio_postgres::Error> {
        let max = self.config.packs.max_attempts;
        let cooldown = self.config.packs.retry_cooldown_mins;
        let rows = self.client.query(
            &format!("SELECT m.hash, m.imdb_id, p.season, t.size_bytes
             FROM imdb_mappings m
             JOIN torrents t ON m.hash = t.hash
             JOIN parsed_torrents p ON m.hash = p.hash
             WHERE m.completed = false AND m.attempts < $1
               AND (m.attempts = 0 OR m.last_processed < NOW() - INTERVAL '{cooldown} minutes')
               AND m.content_type = 'series'
               AND p.season IS NOT NULL AND p.episode IS NULL
             ORDER BY m.last_processed ASC
             LIMIT $2"),
            &[&max, &limit],
        ).await?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))).collect())
    }

    // =========================================================================
    // Streams (shared by Pipeline 5 + 6)
    // =========================================================================

    pub async fn upsert_stream(
        &self,
        torrent_hash: &str,
        imdb_id: &str,
        stream_type: &str,
        size_bytes: i64,
        file_index: Option<i32>,
        season: Option<i32>,
        episode: Option<i32>,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO streams (torrent_hash, imdb_id, stream_type, size_bytes, file_index, season, episode)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (torrent_hash, file_index) DO UPDATE
                 SET imdb_id = $2, stream_type = $3, size_bytes = $4, season = $6, episode = $7",
                &[&torrent_hash, &imdb_id, &stream_type, &size_bytes, &file_index, &season, &episode],
            )
            .await?;
        Ok(())
    }

    // =========================================================================
    // Paginated queries for UI
    // =========================================================================

    /// Reset attempts to 0 for all non-completed records in a table.
    pub async fn reset_attempts(&self, table: &str) -> Result<u64, tokio_postgres::Error> {
        let valid_tables = ["dmm_hashlists", "torrents", "parsed_torrents", "imdb_mappings"];
        if !valid_tables.contains(&table) {
            return Ok(0);
        }
        let sql = format!("UPDATE {table} SET attempts = 0, last_processed = NOW() WHERE completed = false AND attempts > 0");
        let rows = self.client.execute(&sql, &[]).await?;
        Ok(rows)
    }

    pub async fn get_hashlists_page(&self, page: i64, per_page: i64) -> Result<Paginated<serde_json::Value>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self.client.query_one("SELECT COUNT(*) FROM dmm_hashlists", &[]).await?.get(0);
        let rows = self.client.query(
            "SELECT name, completed, attempts, last_processed FROM dmm_hashlists ORDER BY last_processed DESC LIMIT $1 OFFSET $2",
            &[&per_page, &offset],
        ).await?;
        let items = rows.iter().map(|r| {
            let ts: chrono::DateTime<chrono::Utc> = r.get(3);
            serde_json::json!({
                "name": r.get::<_, String>(0),
                "completed": r.get::<_, bool>(1),
                "attempts": r.get::<_, i32>(2),
                "last_processed": ts.to_rfc3339(),
            })
        }).collect();
        Ok(Paginated { items, total, page, per_page })
    }

    pub async fn get_torrents_page(&self, page: i64, per_page: i64) -> Result<Paginated<TorrentRow>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self.client.query_one("SELECT COUNT(*) FROM torrents", &[]).await?.get(0);
        let rows = self.client.query(
            "SELECT hash, filename, size_bytes, completed, attempts, last_processed
             FROM torrents ORDER BY last_processed DESC LIMIT $1 OFFSET $2",
            &[&per_page, &offset],
        ).await?;
        let items = rows.iter().map(|r| {
            let ts: chrono::DateTime<chrono::Utc> = r.get(5);
            TorrentRow { hash: r.get(0), filename: r.get(1), size_bytes: r.get(2), completed: r.get(3), attempts: r.get(4), last_processed: ts.to_rfc3339() }
        }).collect();
        Ok(Paginated { items, total, page, per_page })
    }

    pub async fn get_parsed_page(&self, page: i64, per_page: i64) -> Result<Paginated<ParsedTorrentRow>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self.client.query_one("SELECT COUNT(*) FROM parsed_torrents", &[]).await?.get(0);
        let rows = self.client.query(
            "SELECT hash, title, year, season, episode, completed, attempts, last_processed
             FROM parsed_torrents ORDER BY last_processed DESC LIMIT $1 OFFSET $2",
            &[&per_page, &offset],
        ).await?;
        let items = rows.iter().map(|r| {
            let ts: chrono::DateTime<chrono::Utc> = r.get(7);
            ParsedTorrentRow { hash: r.get(0), title: r.get(1), year: r.get(2), season: r.get(3), episode: r.get(4), completed: r.get(5), attempts: r.get(6), last_processed: ts.to_rfc3339() }
        }).collect();
        Ok(Paginated { items, total, page, per_page })
    }

    pub async fn get_imdb_page(&self, page: i64, per_page: i64) -> Result<Paginated<ImdbMappingRow>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self.client.query_one("SELECT COUNT(*) FROM imdb_mappings", &[]).await?.get(0);
        let rows = self.client.query(
            "SELECT hash, imdb_id, content_type, completed, attempts, last_processed
             FROM imdb_mappings ORDER BY last_processed DESC LIMIT $1 OFFSET $2",
            &[&per_page, &offset],
        ).await?;
        let items = rows.iter().map(|r| {
            let ts: chrono::DateTime<chrono::Utc> = r.get(5);
            ImdbMappingRow { hash: r.get(0), imdb_id: r.get(1), content_type: r.get(2), completed: r.get(3), attempts: r.get(4), last_processed: ts.to_rfc3339() }
        }).collect();
        Ok(Paginated { items, total, page, per_page })
    }

    pub async fn get_streams_page(&self, page: i64, per_page: i64) -> Result<Paginated<StreamRow>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self.client.query_one("SELECT COUNT(*) FROM streams", &[]).await?.get(0);
        let rows = self.client.query(
            "SELECT s.torrent_hash, s.imdb_id, s.stream_type, s.size_bytes, s.file_index,
                    s.season, s.episode, COALESCE(t.filename, ''), s.created_at
             FROM streams s
             LEFT JOIN torrents t ON s.torrent_hash = t.hash
             ORDER BY s.created_at DESC LIMIT $1 OFFSET $2",
            &[&per_page, &offset],
        ).await?;
        let items = rows.iter().map(|r| {
            let ts: chrono::DateTime<chrono::Utc> = r.get(8);
            StreamRow { torrent_hash: r.get(0), imdb_id: r.get(1), stream_type: r.get(2), size_bytes: r.get(3), file_index: r.get(4), season: r.get(5), episode: r.get(6), filename: r.get(7), created_at: ts.to_rfc3339() }
        }).collect();
        Ok(Paginated { items, total, page, per_page })
    }

    // =========================================================================
    // Stats + Queue Depths
    // =========================================================================

    pub async fn counts(&self) -> Result<RecordCounts, tokio_postgres::Error> {
        async fn c(client: &Client, sql: &str) -> Result<i64, tokio_postgres::Error> {
            Ok(client.query_one(sql, &[]).await?.get(0))
        }
        let cl = &*self.client;
        Ok(RecordCounts {
            hashlists: c(cl, "SELECT COUNT(*) FROM dmm_hashlists").await?,
            torrents: c(cl, "SELECT COUNT(*) FROM torrents").await?,
            parsed: c(cl, "SELECT COUNT(*) FROM parsed_torrents").await?,
            imdb: c(cl, "SELECT COUNT(*) FROM imdb_mappings").await?,
            streams: c(cl, "SELECT COUNT(*) FROM streams").await?,
        })
    }

    pub async fn get_queue_depths(&self) -> Result<QueueDepths, tokio_postgres::Error> {
        async fn c(client: &Client, sql: &str) -> Result<i64, tokio_postgres::Error> {
            Ok(client.query_one(sql, &[]).await?.get(0))
        }
        let cl = &*self.client;
        let cfg = &self.config;

        // Each pipeline: completed, unresolved (not done + under max attempts), exhausted (over max)
        let hl_c = c(cl, "SELECT COUNT(*) FROM dmm_hashlists WHERE completed = true").await?;
        let hl_u = c(cl, &format!("SELECT COUNT(*) FROM dmm_hashlists WHERE completed = false AND attempts < {}", cfg.extract.max_attempts)).await?;
        let hl_e = c(cl, &format!("SELECT COUNT(*) FROM dmm_hashlists WHERE completed = false AND attempts >= {}", cfg.extract.max_attempts)).await?;

        let ext_c = c(cl, "SELECT COUNT(*) FROM torrents WHERE completed = true").await?;
        let ext_u = c(cl, &format!("SELECT COUNT(*) FROM torrents WHERE completed = false AND attempts < {}", cfg.parse.max_attempts)).await?;
        let ext_e = c(cl, &format!("SELECT COUNT(*) FROM torrents WHERE completed = false AND attempts >= {}", cfg.parse.max_attempts)).await?;

        let par_c = c(cl, "SELECT COUNT(*) FROM parsed_torrents WHERE completed = true").await?;
        let par_u = c(cl, &format!("SELECT COUNT(*) FROM parsed_torrents WHERE completed = false AND attempts < {}", cfg.imdb.max_attempts)).await?;
        let par_e = c(cl, &format!("SELECT COUNT(*) FROM parsed_torrents WHERE completed = false AND attempts >= {}", cfg.imdb.max_attempts)).await?;

        let imdb_c = c(cl, "SELECT COUNT(*) FROM imdb_mappings WHERE completed = true").await?;
        let imdb_u = c(cl, &format!("SELECT COUNT(*) FROM imdb_mappings WHERE completed = false AND attempts < {}", cfg.singles.max_attempts)).await?;
        let imdb_e = c(cl, &format!("SELECT COUNT(*) FROM imdb_mappings WHERE completed = false AND attempts >= {}", cfg.singles.max_attempts)).await?;

        // Singles + packs split from imdb_mappings
        let sin_c = c(cl, "SELECT COUNT(*) FROM imdb_mappings m JOIN parsed_torrents p ON m.hash = p.hash WHERE m.completed = true AND (m.content_type = 'movie' OR (m.content_type = 'series' AND p.episode IS NOT NULL))").await?;
        let sin_u = c(cl, &format!("SELECT COUNT(*) FROM imdb_mappings m JOIN parsed_torrents p ON m.hash = p.hash WHERE m.completed = false AND m.attempts < {} AND (m.content_type = 'movie' OR (m.content_type = 'series' AND p.episode IS NOT NULL))", cfg.singles.max_attempts)).await?;

        let pak_c = c(cl, "SELECT COUNT(*) FROM imdb_mappings m JOIN parsed_torrents p ON m.hash = p.hash WHERE m.completed = true AND m.content_type = 'series' AND p.season IS NOT NULL AND p.episode IS NULL").await?;
        let pak_u = c(cl, &format!("SELECT COUNT(*) FROM imdb_mappings m JOIN parsed_torrents p ON m.hash = p.hash WHERE m.completed = false AND m.attempts < {} AND m.content_type = 'series' AND p.season IS NOT NULL AND p.episode IS NULL", cfg.packs.max_attempts)).await?;
        let pak_e = c(cl, &format!("SELECT COUNT(*) FROM imdb_mappings m JOIN parsed_torrents p ON m.hash = p.hash WHERE m.completed = false AND m.attempts >= {} AND m.content_type = 'series' AND p.season IS NOT NULL AND p.episode IS NULL", cfg.packs.max_attempts)).await?;

        Ok(QueueDepths {
            hashlists: PipelineStats { unresolved: hl_u, completed: hl_c, exhausted: hl_e },
            extract: PipelineStats { unresolved: ext_u, completed: ext_c, exhausted: ext_e },
            parse: PipelineStats { unresolved: par_u, completed: par_c, exhausted: par_e },
            imdb: PipelineStats { unresolved: imdb_u, completed: imdb_c, exhausted: imdb_e },
            singles: PipelineStats { unresolved: sin_u, completed: sin_c, exhausted: 0 },
            packs: PipelineStats { unresolved: pak_u, completed: pak_c, exhausted: pak_e },
        })
    }
}
