use serde::{Deserialize, Serialize};

fn default_collect_server_stats() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkloadConfig {
    #[serde(rename = "num_queries")]
    pub num_queries: Option<u64>,
    #[serde(rename = "num_keys")]
    pub num_keys: u64,
    #[serde(rename = "zipf_exponent")]
    pub zipf_exponent: f64,
    pub namespace: String,
    pub name: String,
    #[serde(rename = "max_concurrency")]
    pub max_concurrency: String,
    #[serde(rename = "seed")]
    pub seed: Option<u64>,
    #[serde(rename = "background_runtime_core_ids")]
    pub background_runtime_core_ids: Vec<u32>,
    #[serde(rename = "fake_transactions")]
    pub fake_transactions: bool,
    #[serde(rename = "workload_type")]
    pub workload_type: String,
    /// Only the foreground generator should collect destructive server stats.
    #[serde(default = "default_collect_server_stats")]
    pub collect_server_stats: bool,
}
