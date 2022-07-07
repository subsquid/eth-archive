use crate::config::{Config, IngestConfig};
use crate::db::DbHandle;
use crate::options::Options;
use crate::{Error, Result};

pub struct Ingester {
    db_handle: DbHandle,
    cfg: IngestConfig,
    rpc_client: RpcClient,
}

impl Ingester {
    pub async fn new(options: &Options) -> Result<Self> {
        let config = tokio::fs::read_to_string(&options.cfg_path)
            .await
            .map_err(Error::ReadConfigFile)?;

        let config: Config = toml::de::from_str(&config).map_err(Error::ParseConfig)?;

        let db_handle = DbHandle::new(options, &config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;

        Ok(Self {
            db_handle,
            cfg: config.ingest,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let from_block = self.initial_sync().await?;
        todo!();
    }

    pub async fn initial_sync(&self) -> Result<u64> {
        let from_block =  match self.cfg.from_block {
            Some(from_block) => from_block,
            None => self.last_synched_block().await?,
        };
        let to_block = match self.cfg.to_block {
            Some(to_block) => to_block,
            None => self.rpc_client.get_bestblock().await?,
        };
    }

    pub async fn last_synched_block(&self) -> Result<u64> {
        
    }
}
