use crate::ingestor::HashlistStatus;
use serde::Serialize;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

/// Async PostgreSQL database handle.
#[derive(Clone)]
pub struct Db {
    client: Arc<Client>,
}

/// A row from the `torrents` table.
#[derive(Debug, Clone, Serialize)]
pub struct TorrentRow {
    pub hash: String,
    pub filename: String,
    pub size_bytes: i64,
    pub last_updated: String,
}

/// A row from the `parsed_metadata` table.
#[derive(Debug, Clone, Serialize)]
pub struct ParsedMetadataRow {
    pub hash: String,
    pub title: String,
    pub year: Option<i32>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    pub last_updated: String,
}

/// Paginated response wrapper.
#[derive(Debug, Clone, Serialize)]
pub struct Paginated<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

/// A row from the `imdb_mappings` table.
#[derive(Debug, Clone, Serialize)]
pub struct ImdbMappingRow {
    pub hash: String,
    pub imdb_id: String,
    pub content_type: String,
    pub last_updated: String,
}

pub struct RecordCounts {
    pub total_torrents: i64,
    pub total_parsed: i64,
    pub total_imdb: i64,
    pub total_streams: i64,
}

/// A row from the `streams` table — the final output table.
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
    pub last_updated: String,
}

/// An entry ready for the singles worker (movie or single episode).
#[derive(Debug, Clone)]
pub struct SinglesCandidate {
    pub hash: String,
    pub imdb_id: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub season: Option<i32>,
    pub episode: Option<i32>,
}

/// An entry ready for the season pack worker.
#[derive(Debug, Clone)]
pub struct SeasonPackCandidate {
    pub hash: String,
    pub imdb_id: String,
    pub season: i32,
    pub size_bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineStats {
    pub unresolved: i64,
    pub completed: i64,
    pub failed: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueDepths {
    pub hashlist: PipelineStats,
    pub imdb: PipelineStats,
    pub singles: PipelineStats,
    pub packs: PipelineStats,
    pub dht: PipelineStats,
}

impl Db {
    /// Connect to PostgreSQL and initialise schema.
    pub async fn connect(database_url: &str) -> Result<Self, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;

        // Drive the connection in the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {e}");
            }
        });

        let db = Self {
            client: Arc::new(client),
        };
        db.init_schema().await?;
        Ok(db)
    }

    async fn init_schema(&self) -> Result<(), tokio_postgres::Error> {
        self.client
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS torrents (
                hash TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS parsed_metadata (
                hash TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                year INTEGER,
                season INTEGER,
                episode INTEGER,
                last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS imdb_mappings (
                hash TEXT PRIMARY KEY,
                imdb_id TEXT NOT NULL,
                content_type TEXT NOT NULL DEFAULT 'movie',
                last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS hashlists (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS processed_names (
                name TEXT PRIMARY KEY
            );

            -- Trigger function for auto-updating last_updated on UPDATE
            CREATE OR REPLACE FUNCTION update_last_updated()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.last_updated = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS torrents_update_last_updated ON torrents;
            CREATE TRIGGER torrents_update_last_updated
                BEFORE UPDATE ON torrents
                FOR EACH ROW EXECUTE FUNCTION update_last_updated();

            DROP TRIGGER IF EXISTS parsed_metadata_update_last_updated ON parsed_metadata;
            CREATE TRIGGER parsed_metadata_update_last_updated
                BEFORE UPDATE ON parsed_metadata
                FOR EACH ROW EXECUTE FUNCTION update_last_updated();

            DROP TRIGGER IF EXISTS imdb_mappings_update_last_updated ON imdb_mappings;
            CREATE TRIGGER imdb_mappings_update_last_updated
                BEFORE UPDATE ON imdb_mappings
                FOR EACH ROW EXECUTE FUNCTION update_last_updated();

            CREATE TABLE IF NOT EXISTS imdb_failures (
                hash TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                year INTEGER,
                attempts INTEGER NOT NULL DEFAULT 1,
                last_attempt TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS streams (
                id SERIAL PRIMARY KEY,
                torrent_hash TEXT NOT NULL,
                imdb_id TEXT NOT NULL,
                stream_type TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                file_index INTEGER,
                season INTEGER,
                episode INTEGER,
                last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(torrent_hash, file_index)
            );

            CREATE TABLE IF NOT EXISTS streams_processed (
                hash TEXT PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS packs_failures (
                hash TEXT PRIMARY KEY,
                attempts INTEGER NOT NULL DEFAULT 1,
                last_attempt TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS dht_queue (
                hash TEXT PRIMARY KEY,
                imdb_id TEXT NOT NULL,
                season INTEGER NOT NULL,
                size_bytes BIGINT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_attempt TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_torrents_last_updated ON torrents(last_updated DESC);
            CREATE INDEX IF NOT EXISTS idx_parsed_metadata_last_updated ON parsed_metadata(last_updated DESC);
            CREATE INDEX IF NOT EXISTS idx_imdb_mappings_last_updated ON imdb_mappings(last_updated DESC);
            CREATE INDEX IF NOT EXISTS idx_imdb_failures_attempts ON imdb_failures(attempts);
            CREATE INDEX IF NOT EXISTS idx_streams_imdb_id ON streams(imdb_id);
            CREATE INDEX IF NOT EXISTS idx_streams_last_updated ON streams(last_updated DESC);
            ",
            )
            .await?;
        Ok(())
    }

    // =========================================================================
    // Torrents
    // =========================================================================

    /// Upsert a torrent entry. On conflict (same hash), update filename/size.
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

    /// Batch upsert torrents.
    pub async fn upsert_torrents(
        &self,
        entries: &[(String, String, i64)],
    ) -> Result<(), tokio_postgres::Error> {
        for (hash, filename, size_bytes) in entries {
            self.upsert_torrent(hash, filename, *size_bytes).await?;
        }
        Ok(())
    }

    /// Get torrents paginated, ordered by last_updated DESC.
    pub async fn get_torrents_page(
        &self,
        page: i64,
        per_page: i64,
    ) -> Result<Paginated<TorrentRow>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM torrents", &[])
            .await?
            .get(0);
        let rows = self
            .client
            .query(
                "SELECT hash, filename, size_bytes, last_updated
                 FROM torrents ORDER BY last_updated DESC LIMIT $1 OFFSET $2",
                &[&per_page, &offset],
            )
            .await?;

        let items = rows
            .iter()
            .map(|r| {
                let ts: chrono::DateTime<chrono::Utc> = r.get(3);
                TorrentRow {
                    hash: r.get(0),
                    filename: r.get(1),
                    size_bytes: r.get(2),
                    last_updated: ts.to_rfc3339(),
                }
            })
            .collect();

        Ok(Paginated {
            items,
            total,
            page,
            per_page,
        })
    }

    pub async fn count_torrents(&self) -> Result<i64, tokio_postgres::Error> {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM torrents", &[])
            .await?;
        Ok(row.get(0))
    }

    // =========================================================================
    // Parsed Metadata
    // =========================================================================

    /// Upsert parsed metadata. On conflict (same hash), update fields.
    pub async fn upsert_parsed_metadata(
        &self,
        hash: &str,
        title: &str,
        year: Option<i32>,
        season: Option<i32>,
        episode: Option<i32>,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO parsed_metadata (hash, title, year, season, episode)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (hash) DO UPDATE
                 SET title = $2, year = $3, season = $4, episode = $5",
                &[&hash, &title, &year, &season, &episode],
            )
            .await?;
        Ok(())
    }

    /// Get parsed metadata paginated, ordered by last_updated DESC.
    pub async fn get_parsed_metadata_page(
        &self,
        page: i64,
        per_page: i64,
    ) -> Result<Paginated<ParsedMetadataRow>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM parsed_metadata", &[])
            .await?
            .get(0);
        let rows = self
            .client
            .query(
                "SELECT hash, title, year, season, episode, last_updated
                 FROM parsed_metadata ORDER BY last_updated DESC LIMIT $1 OFFSET $2",
                &[&per_page, &offset],
            )
            .await?;

        let items = rows
            .iter()
            .map(|r| {
                let ts: chrono::DateTime<chrono::Utc> = r.get(5);
                ParsedMetadataRow {
                    hash: r.get(0),
                    title: r.get(1),
                    year: r.get(2),
                    season: r.get(3),
                    episode: r.get(4),
                    last_updated: ts.to_rfc3339(),
                }
            })
            .collect();

        Ok(Paginated {
            items,
            total,
            page,
            per_page,
        })
    }

    pub async fn count_parsed_metadata(&self) -> Result<i64, tokio_postgres::Error> {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM parsed_metadata", &[])
            .await?;
        Ok(row.get(0))
    }

    // =========================================================================
    // IMDb Mappings
    // =========================================================================

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

    /// Get parsed_metadata rows that have no corresponding imdb_mappings entry yet.
    pub async fn get_unmapped_parsed_entries(
        &self,
        limit: i64,
    ) -> Result<Vec<ParsedMetadataRow>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT p.hash, p.title, p.year, p.season, p.episode, p.last_updated
                 FROM parsed_metadata p
                 LEFT JOIN imdb_mappings m ON p.hash = m.hash
                 WHERE m.hash IS NULL
                 ORDER BY p.last_updated DESC
                 LIMIT $1",
                &[&limit],
            )
            .await?;

        let items = rows
            .iter()
            .map(|r| {
                let ts: chrono::DateTime<chrono::Utc> = r.get(5);
                ParsedMetadataRow {
                    hash: r.get(0),
                    title: r.get(1),
                    year: r.get(2),
                    season: r.get(3),
                    episode: r.get(4),
                    last_updated: ts.to_rfc3339(),
                }
            })
            .collect();

        Ok(items)
    }

    pub async fn get_imdb_mappings_page(
        &self,
        page: i64,
        per_page: i64,
    ) -> Result<Paginated<ImdbMappingRow>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM imdb_mappings", &[])
            .await?
            .get(0);
        let rows = self
            .client
            .query(
                "SELECT hash, imdb_id, content_type, last_updated
                 FROM imdb_mappings ORDER BY last_updated DESC LIMIT $1 OFFSET $2",
                &[&per_page, &offset],
            )
            .await?;

        let items = rows
            .iter()
            .map(|r| {
                let ts: chrono::DateTime<chrono::Utc> = r.get(3);
                ImdbMappingRow {
                    hash: r.get(0),
                    imdb_id: r.get(1),
                    content_type: r.get(2),
                    last_updated: ts.to_rfc3339(),
                }
            })
            .collect();

        Ok(Paginated {
            items,
            total,
            page,
            per_page,
        })
    }

    pub async fn count_imdb_mappings(&self) -> Result<i64, tokio_postgres::Error> {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM imdb_mappings", &[])
            .await?;
        Ok(row.get(0))
    }

    // =========================================================================
    // Hashlists
    // =========================================================================

    pub async fn insert_hashlist(&self, h: &HashlistStatus) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO hashlists (name, record_count, status) VALUES ($1, $2, $3)",
                &[&h.name, &(h.record_count as i32), &h.status],
            )
            .await?;
        Ok(())
    }

    pub async fn count_hashlists(&self) -> Result<i64, tokio_postgres::Error> {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM hashlists", &[])
            .await?;
        Ok(row.get(0))
    }

    pub async fn count_hashlists_done(&self) -> Result<i64, tokio_postgres::Error> {
        let row = self
            .client
            .query_one(
                "SELECT COUNT(*) FROM hashlists WHERE status = 'done'",
                &[],
            )
            .await?;
        Ok(row.get(0))
    }

    // =========================================================================
    // Processed names
    // =========================================================================

    pub async fn is_processed(&self, name: &str) -> Result<bool, tokio_postgres::Error> {
        let row = self
            .client
            .query_one(
                "SELECT COUNT(*) FROM processed_names WHERE name = $1",
                &[&name],
            )
            .await?;
        let count: i64 = row.get(0);
        Ok(count > 0)
    }

    pub async fn mark_processed(&self, name: &str) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO processed_names (name) VALUES ($1) ON CONFLICT DO NOTHING",
                &[&name],
            )
            .await?;
        Ok(())
    }

    // =========================================================================
    // Stats
    // =========================================================================

    // =========================================================================
    // IMDb Failures
    // =========================================================================

    /// Get unmapped parsed entries excluding permanently failed and recently retried.
    /// Entries with >= 3 attempts are permanently skipped.
    /// Entries retried within the last 30 minutes are skipped (cooldown).
    pub async fn get_unmapped_entries_for_resolution(
        &self,
        limit: i64,
    ) -> Result<Vec<ParsedMetadataRow>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT p.hash, p.title, p.year, p.season, p.episode, p.last_updated
                 FROM parsed_metadata p
                 LEFT JOIN imdb_mappings m ON p.hash = m.hash
                 LEFT JOIN imdb_failures f ON p.hash = f.hash
                 WHERE m.hash IS NULL
                   AND (f.hash IS NULL
                        OR (f.attempts < 3
                            AND f.last_attempt < NOW() - INTERVAL '30 minutes'))
                 ORDER BY p.last_updated DESC
                 LIMIT $1",
                &[&limit],
            )
            .await?;

        let items = rows
            .iter()
            .map(|r| {
                let ts: chrono::DateTime<chrono::Utc> = r.get(5);
                ParsedMetadataRow {
                    hash: r.get(0),
                    title: r.get(1),
                    year: r.get(2),
                    season: r.get(3),
                    episode: r.get(4),
                    last_updated: ts.to_rfc3339(),
                }
            })
            .collect();

        Ok(items)
    }

    /// Record a failed IMDb resolution attempt. Increments attempts on conflict.
    pub async fn mark_imdb_failure(
        &self,
        hash: &str,
        title: &str,
        year: Option<i32>,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO imdb_failures (hash, title, year, attempts, last_attempt)
                 VALUES ($1, $2, $3, 1, NOW())
                 ON CONFLICT (hash) DO UPDATE
                 SET attempts = imdb_failures.attempts + 1, last_attempt = NOW()",
                &[&hash, &title, &year],
            )
            .await?;
        Ok(())
    }

    // =========================================================================
    // Queue Depths
    // =========================================================================

    pub async fn get_queue_depths(&self) -> Result<QueueDepths, tokio_postgres::Error> {
        // Helper to run a count query
        async fn count(client: &tokio_postgres::Client, sql: &str) -> Result<i64, tokio_postgres::Error> {
            Ok(client.query_one(sql, &[]).await?.get(0))
        }
        let c = &*self.client;

        // Hashlist: unresolved = total html files not yet processed
        // We don't track total easily, so use hashlists table
        let hl_completed = count(c, "SELECT COUNT(*) FROM processed_names").await?;
        let hl_failed = count(c, "SELECT COUNT(*) FROM hashlists WHERE status LIKE 'error%'").await?;
        let hl_total = count(c, "SELECT COUNT(*) FROM hashlists").await?;
        let hl_unresolved = (hl_total - hl_completed - hl_failed).max(0);

        // IMDb
        let imdb_completed = count(c, "SELECT COUNT(*) FROM imdb_mappings").await?;
        let imdb_failed = count(c, "SELECT COUNT(*) FROM imdb_failures WHERE attempts >= 3").await?;
        let imdb_unresolved = count(c,
            "SELECT COUNT(*) FROM parsed_metadata p
             LEFT JOIN imdb_mappings m ON p.hash = m.hash
             LEFT JOIN imdb_failures f ON p.hash = f.hash
             WHERE m.hash IS NULL AND (f.hash IS NULL OR f.attempts < 3)"
        ).await?;

        // Singles
        let singles_completed = count(c,
            "SELECT COUNT(*) FROM streams_processed sp
             JOIN imdb_mappings m ON sp.hash = m.hash
             JOIN parsed_metadata p ON sp.hash = p.hash
             WHERE m.content_type = 'movie'
                OR (m.content_type = 'series' AND p.season IS NOT NULL AND p.episode IS NOT NULL)"
        ).await?;
        let singles_unresolved = count(c,
            "SELECT COUNT(*) FROM imdb_mappings m
             JOIN parsed_metadata p ON m.hash = p.hash
             LEFT JOIN streams_processed sp ON m.hash = sp.hash
             WHERE sp.hash IS NULL
               AND (m.content_type = 'movie'
                    OR (m.content_type = 'series' AND p.season IS NOT NULL AND p.episode IS NOT NULL))"
        ).await?;

        // Packs
        let packs_completed = count(c,
            "SELECT COUNT(*) FROM streams_processed sp
             JOIN imdb_mappings m ON sp.hash = m.hash
             JOIN parsed_metadata p ON sp.hash = p.hash
             WHERE m.content_type = 'series'
               AND p.season IS NOT NULL AND p.episode IS NULL"
        ).await?;
        let packs_failed = count(c,
            "SELECT COUNT(*) FROM packs_failures WHERE attempts >= 3"
        ).await?;
        let packs_unresolved = count(c,
            "SELECT COUNT(*) FROM imdb_mappings m
             JOIN parsed_metadata p ON m.hash = p.hash
             LEFT JOIN streams_processed sp ON m.hash = sp.hash
             LEFT JOIN packs_failures pf ON m.hash = pf.hash
             WHERE sp.hash IS NULL
               AND m.content_type = 'series'
               AND p.season IS NOT NULL AND p.episode IS NULL
               AND (pf.hash IS NULL OR pf.attempts < 3)"
        ).await?;

        // DHT queue
        let dht_unresolved = count(c,
            "SELECT COUNT(*) FROM dht_queue WHERE attempts < 5 AND (last_attempt IS NULL OR last_attempt < NOW() - INTERVAL '1 hour')"
        ).await?;
        let dht_completed = count(c,
            "SELECT COUNT(*) FROM dht_queue WHERE attempts = -1"
        ).await?;
        let dht_failed = count(c,
            "SELECT COUNT(*) FROM dht_queue WHERE attempts >= 5"
        ).await?;

        Ok(QueueDepths {
            hashlist: PipelineStats { unresolved: hl_unresolved, completed: hl_completed, failed: hl_failed },
            imdb: PipelineStats { unresolved: imdb_unresolved, completed: imdb_completed, failed: imdb_failed },
            singles: PipelineStats { unresolved: singles_unresolved, completed: singles_completed, failed: 0 },
            packs: PipelineStats { unresolved: packs_unresolved, completed: packs_completed, failed: packs_failed },
            dht: PipelineStats { unresolved: dht_unresolved, completed: dht_completed, failed: dht_failed },
        })
    }

    // =========================================================================
    // Streams
    // =========================================================================

    /// Insert a stream record. On conflict (same hash + file_index), update.
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

    /// Mark a hash as processed for the streams pipeline.
    pub async fn mark_pack_failure(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO packs_failures (hash, attempts, last_attempt)
                 VALUES ($1, 1, NOW())
                 ON CONFLICT (hash) DO UPDATE
                 SET attempts = packs_failures.attempts + 1, last_attempt = NOW()",
                &[&hash],
            )
            .await?;
        Ok(())
    }

    /// Add a hash to the DHT lookup queue (packs that couldn't be resolved by caches/RD).
    pub async fn enqueue_dht_lookup(
        &self,
        hash: &str,
        imdb_id: &str,
        season: i32,
        size_bytes: i64,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO dht_queue (hash, imdb_id, season, size_bytes)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (hash) DO NOTHING",
                &[&hash, &imdb_id, &season, &size_bytes],
            )
            .await?;
        Ok(())
    }

    /// Get hashes from the DHT queue for processing.
    /// Skips recently attempted (1hr cooldown) and permanently failed (3+ attempts).
    pub async fn get_dht_queue_candidates(
        &self,
        limit: i64,
    ) -> Result<Vec<SeasonPackCandidate>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT hash, imdb_id, season, size_bytes FROM dht_queue
                 WHERE attempts < 5
                   AND (last_attempt IS NULL OR last_attempt < NOW() - INTERVAL '1 hour')
                 ORDER BY created_at ASC
                 LIMIT $1",
                &[&limit],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| SeasonPackCandidate {
                hash: r.get(0),
                imdb_id: r.get(1),
                season: r.get(2),
                size_bytes: r.get(3),
            })
            .collect())
    }

    /// Mark a DHT queue entry as attempted (bump attempts, update timestamp).
    pub async fn mark_dht_attempted(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE dht_queue SET attempts = attempts + 1, last_attempt = NOW() WHERE hash = $1",
                &[&hash],
            )
            .await?;
        Ok(())
    }

    /// Get the current attempt count for a DHT queue entry.
    pub async fn get_dht_attempts(&self, hash: &str) -> Result<i32, tokio_postgres::Error> {
        let row = self
            .client
            .query_one(
                "SELECT attempts FROM dht_queue WHERE hash = $1",
                &[&hash],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Reset packs_failures cooldown so it re-enters the packs cache worker.
    pub async fn reset_pack_failure_cooldown(
        &self,
        hash: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE packs_failures SET last_attempt = NOW() - INTERVAL '2 hours' WHERE hash = $1",
                &[&hash],
            )
            .await?;
        Ok(())
    }

    /// Delete from DHT queue (used when recycling back to packs cache).
    pub async fn delete_from_dht_queue(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute("DELETE FROM dht_queue WHERE hash = $1", &[&hash])
            .await?;
        Ok(())
    }

    /// Mark a DHT queue entry as resolved (keep for history).
    pub async fn mark_dht_resolved(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "UPDATE dht_queue SET attempts = -1 WHERE hash = $1",
                &[&hash],
            )
            .await?;
        Ok(())
    }

    /// Get DHT queue entries for the UI table (all entries with their status).
    pub async fn get_dht_queue_page(
        &self,
        page: i64,
        per_page: i64,
    ) -> Result<Paginated<serde_json::Value>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM dht_queue", &[])
            .await?
            .get(0);
        let rows = self
            .client
            .query(
                "SELECT dq.hash, dq.imdb_id, dq.season, dq.attempts, dq.last_attempt, dq.created_at,
                        COALESCE(t.filename, '') as filename,
                        CASE WHEN dq.attempts = -1 THEN 'resolved'
                             WHEN dq.attempts >= 5 THEN 'lost'
                             ELSE 'pending' END as status
                 FROM dht_queue dq
                 LEFT JOIN torrents t ON dq.hash = t.hash
                 ORDER BY dq.attempts DESC, dq.created_at DESC
                 LIMIT $1 OFFSET $2",
                &[&per_page, &offset],
            )
            .await?;

        let items = rows
            .iter()
            .map(|r| {
                let last_attempt: Option<chrono::DateTime<chrono::Utc>> = r.get(4);
                let created: chrono::DateTime<chrono::Utc> = r.get(5);
                serde_json::json!({
                    "hash": r.get::<_, String>(0),
                    "imdb_id": r.get::<_, String>(1),
                    "season": r.get::<_, i32>(2),
                    "attempts": r.get::<_, i32>(3),
                    "last_attempt": last_attempt.map(|t| t.to_rfc3339()),
                    "created_at": created.to_rfc3339(),
                    "filename": r.get::<_, String>(6),
                    "status": r.get::<_, String>(7),
                })
            })
            .collect();

        Ok(Paginated {
            items,
            total,
            page,
            per_page,
        })
    }

    pub async fn mark_stream_processed(&self, hash: &str) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO streams_processed (hash) VALUES ($1) ON CONFLICT DO NOTHING",
                &[&hash],
            )
            .await?;
        Ok(())
    }

    /// Get singles candidates: movies OR episodes (series with season+episode).
    /// Excludes already-processed hashes.
    pub async fn get_singles_candidates(
        &self,
        limit: i64,
    ) -> Result<Vec<SinglesCandidate>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT t.hash, m.imdb_id, m.content_type, t.size_bytes, p.season, p.episode
                 FROM torrents t
                 JOIN imdb_mappings m ON t.hash = m.hash
                 JOIN parsed_metadata p ON t.hash = p.hash
                 LEFT JOIN streams_processed sp ON t.hash = sp.hash
                 WHERE sp.hash IS NULL
                   AND (
                     m.content_type = 'movie'
                     OR (m.content_type = 'series' AND p.season IS NOT NULL AND p.episode IS NOT NULL)
                   )
                 LIMIT $1",
                &[&limit],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| SinglesCandidate {
                hash: r.get(0),
                imdb_id: r.get(1),
                content_type: r.get(2),
                size_bytes: r.get(3),
                season: r.get(4),
                episode: r.get(5),
            })
            .collect())
    }

    /// Get season pack candidates: series with season but NO episode.
    /// Excludes already-processed hashes and recently failed ones (cooldown 1hr, max 3 attempts).
    pub async fn get_season_pack_candidates(
        &self,
        limit: i64,
    ) -> Result<Vec<SeasonPackCandidate>, tokio_postgres::Error> {
        let rows = self
            .client
            .query(
                "SELECT t.hash, m.imdb_id, p.season, t.size_bytes
                 FROM torrents t
                 JOIN imdb_mappings m ON t.hash = m.hash
                 JOIN parsed_metadata p ON t.hash = p.hash
                 LEFT JOIN streams_processed sp ON t.hash = sp.hash
                 LEFT JOIN dht_queue dq ON t.hash = dq.hash
                 WHERE sp.hash IS NULL
                   AND m.content_type = 'series'
                   AND p.season IS NOT NULL
                   AND p.episode IS NULL
                   AND (dq.hash IS NULL
                        OR (dq.attempts > 0 AND dq.attempts < 5
                            AND dq.last_attempt < NOW() - INTERVAL '1 hour'))
                 ORDER BY p.last_updated ASC
                 LIMIT $1",
                &[&limit],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| SeasonPackCandidate {
                hash: r.get(0),
                imdb_id: r.get(1),
                season: r.get(2),
                size_bytes: r.get(3),
            })
            .collect())
    }

    pub async fn get_streams_page(
        &self,
        page: i64,
        per_page: i64,
    ) -> Result<Paginated<StreamRow>, tokio_postgres::Error> {
        let offset = (page - 1) * per_page;
        let total: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM streams", &[])
            .await?
            .get(0);
        let rows = self
            .client
            .query(
                "SELECT s.torrent_hash, s.imdb_id, s.stream_type, s.size_bytes, s.file_index,
                        s.season, s.episode, COALESCE(t.filename, ''), s.last_updated
                 FROM streams s
                 LEFT JOIN torrents t ON s.torrent_hash = t.hash
                 ORDER BY s.last_updated DESC LIMIT $1 OFFSET $2",
                &[&per_page, &offset],
            )
            .await?;

        let items = rows
            .iter()
            .map(|r| {
                let ts: chrono::DateTime<chrono::Utc> = r.get(8);
                StreamRow {
                    torrent_hash: r.get(0),
                    imdb_id: r.get(1),
                    stream_type: r.get(2),
                    size_bytes: r.get(3),
                    file_index: r.get(4),
                    season: r.get(5),
                    episode: r.get(6),
                    filename: r.get(7),
                    last_updated: ts.to_rfc3339(),
                }
            })
            .collect();

        Ok(Paginated {
            items,
            total,
            page,
            per_page,
        })
    }

    pub async fn count_streams(&self) -> Result<i64, tokio_postgres::Error> {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM streams", &[])
            .await?;
        Ok(row.get(0))
    }

    pub async fn count_streams_processed(&self) -> Result<i64, tokio_postgres::Error> {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM streams_processed", &[])
            .await?;
        Ok(row.get(0))
    }

    // =========================================================================
    // Stats
    // =========================================================================

    pub async fn counts(&self) -> Result<RecordCounts, tokio_postgres::Error> {
        let t: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM torrents", &[])
            .await?
            .get(0);
        let p: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM parsed_metadata", &[])
            .await?
            .get(0);
        let i: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM imdb_mappings", &[])
            .await?
            .get(0);
        let s: i64 = self
            .client
            .query_one("SELECT COUNT(*) FROM streams", &[])
            .await?
            .get(0);
        Ok(RecordCounts {
            total_torrents: t,
            total_parsed: p,
            total_imdb: i,
            total_streams: s,
        })
    }
}
