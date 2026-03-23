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
    pub last_updated: String,
}

pub struct RecordCounts {
    pub total_torrents: i64,
    pub total_parsed: i64,
    pub total_imdb: i64,
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

            CREATE INDEX IF NOT EXISTS idx_torrents_last_updated ON torrents(last_updated DESC);
            CREATE INDEX IF NOT EXISTS idx_parsed_metadata_last_updated ON parsed_metadata(last_updated DESC);
            CREATE INDEX IF NOT EXISTS idx_imdb_mappings_last_updated ON imdb_mappings(last_updated DESC);
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
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO imdb_mappings (hash, imdb_id)
                 VALUES ($1, $2)
                 ON CONFLICT (hash) DO UPDATE SET imdb_id = $2",
                &[&hash, &imdb_id],
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
                "SELECT hash, imdb_id, last_updated
                 FROM imdb_mappings ORDER BY last_updated DESC LIMIT $1 OFFSET $2",
                &[&per_page, &offset],
            )
            .await?;

        let items = rows
            .iter()
            .map(|r| {
                let ts: chrono::DateTime<chrono::Utc> = r.get(2);
                ImdbMappingRow {
                    hash: r.get(0),
                    imdb_id: r.get(1),
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
        Ok(RecordCounts {
            total_torrents: t,
            total_parsed: p,
            total_imdb: i,
        })
    }
}
