use crate::error::{Error, MigrationPhase, Result};
use crate::tls;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio_postgres::config::SslMode;

#[derive(Clone, Debug)]
pub struct DbArgs {
    pub host: Arc<str>,
    pub port: u16,
    pub user: Arc<str>,
    pub pass: Arc<str>,
}

#[derive(Clone, Debug)]
pub struct MigrationState {
    pub db: String,
    pub size: u64,
    pub phase: MigrationPhase,
    pub step: u8,
    pub total_steps: u8,
    pub display: String,
    pub error: Option<String>,
    pub regular_completed_at: Option<Instant>,
    pub started_at: Option<Instant>,
    pub finished_at: Option<Instant>,
}

impl MigrationState {
    #[must_use]
    pub fn new(db: impl Into<String>, size: u64) -> Self {
        let db = db.into();

        Self {
            display: "waiting".to_string(),
            db,
            size,
            phase: MigrationPhase::Pending,
            step: 0,
            total_steps: 4,
            error: None,
            regular_completed_at: None,
            started_at: None,
            finished_at: None,
        }
    }

    pub fn set_phase(&mut self, phase: MigrationPhase, step: u8, display: impl Into<String>) {
        if self.started_at.is_none()
            && phase != MigrationPhase::Pending
            && phase != MigrationPhase::DelayedDumping
        {
            self.started_at = Some(Instant::now());
        }
        if (phase == MigrationPhase::Complete || phase == MigrationPhase::Failed)
            && self.finished_at.is_none()
        {
            self.finished_at = Some(Instant::now());
        }
        self.phase = phase;
        self.step = step;
        self.display = display.into();
    }

    pub fn mark_regular_done(&mut self) {
        self.regular_completed_at = Some(Instant::now());
    }

    pub fn fail(&mut self, error: impl Into<String>) {
        let error = error.into();

        self.phase = MigrationPhase::Failed;
        self.display.clone_from(&error);
        self.error = Some(error);
        if self.finished_at.is_none() {
            self.finished_at = Some(Instant::now());
        }
    }

    #[must_use]
    pub fn percent(&self) -> u8 {
        if self.total_steps == 0 {
            return 0;
        }

        let percent = u16::from(self.step).saturating_mul(100) / u16::from(self.total_steps);
        percent.min(100) as u8
    }
}

#[derive(Hash, PartialEq, Eq, Clone)]
pub struct PoolKey {
    pub host: Arc<str>,
    pub port: u16,
    pub user: Arc<str>,
    pub db: Arc<str>,
}

#[derive(Clone)]
pub struct PoolCache {
    inner: Arc<AsyncMutex<HashMap<PoolKey, Arc<tokio_postgres::Client>>>>,
    pub ssl_mode: SslMode,
}

impl PoolCache {
    #[must_use]
    pub fn new(ssl_mode: SslMode) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(HashMap::new())),
            ssl_mode,
        }
    }

    pub async fn get(&self, args: &DbArgs, db: &str) -> Result<Arc<tokio_postgres::Client>> {
        let key = PoolKey {
            host: args.host.clone(),
            port: args.port,
            user: args.user.clone(),
            db: db.into(),
        };

        if let Some(client) = self.inner.lock().await.get(&key) {
            return Ok(client.clone());
        }

        let mut config = tokio_postgres::Config::new();
        config
            .host(&*args.host)
            .port(args.port)
            .user(&*args.user)
            .password(&*args.pass)
            .dbname(db)
            .ssl_mode(self.ssl_mode);

        let tls = tls::make_tls();
        let (client, connection) =
            tokio::time::timeout(Duration::from_secs(30), config.connect(tls))
                .await
                .map_err(|_| {
                    Error::Timeout(format!("to {} database {}", &*args.host, db).into())
                })??;

        let db_name = db.to_string();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Database connection error for {db_name}: {e}");
            }
        });

        let client = Arc::new(client);
        self.inner.lock().await.insert(key, client.clone());

        Ok(client)
    }

    pub async fn clear(&self) {
        self.inner.lock().await.clear();
    }
}
