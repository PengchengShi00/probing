use super::ApiClient;
use crate::utils::error::Result;
use probing_proto::prelude::DataFrame;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct EngineInfo {
    pub engine_id: String,
    pub engine_type: String,
    pub router_addr: String,
    pub metrics_url: String,
    pub framework: String,
    pub status: String,
    #[serde(default)]
    pub last_scrape_error: Option<String>,
    #[serde(default)]
    pub last_normalized: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineListResponse {
    pub engines: Vec<EngineInfo>,
}

impl ApiClient {
    pub async fn fetch_inference_engines(&self) -> Result<EngineListResponse> {
        let response = self
            .get_request("/apis/pythonext/engines/snapshot")
            .await?;
        Self::parse_json(&response)
    }

    pub async fn scrape_inference_engines(&self) -> Result<String> {
        self.get_request("/apis/pythonext/engines/scrape").await
    }

    pub async fn fetch_inference_engine_metrics(&self, limit: i64) -> Result<DataFrame> {
        self.execute_query(&format!(
            "SELECT timestamp_ns, engine_id, engine_type, metric_name, metric_value, labels \
             FROM python.inference_engine_metric \
             WHERE metric_name LIKE 'normalized.%' \
             ORDER BY timestamp_ns DESC LIMIT {limit}"
        ))
        .await
    }

    pub async fn fetch_inference_engine_raw_metrics(&self, limit: i64) -> Result<DataFrame> {
        self.execute_query(&format!(
            "SELECT timestamp_ns, engine_id, engine_type, metric_name, metric_value, labels \
             FROM python.inference_engine_metric \
             WHERE metric_name NOT LIKE 'normalized.%' \
             ORDER BY timestamp_ns DESC LIMIT {limit}"
        ))
        .await
    }
}
