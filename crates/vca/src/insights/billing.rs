//! How each tool is paid for, from `<data home>/.vca/billing.json`.
//!
//! The file is optional. Without it every provider is `unknown`, which the
//! UI shows as "API-equivalent value" rather than spend.
//!
//! ```json
//! {"providers": {
//!   "claude": {"billing_mode": "subscription", "plan": "Max", "monthly_fee_usd": 200,
//!              "windows": [{"name": "5h", "minutes": 300}, {"name": "weekly", "minutes": 10080}]},
//!   "opencode": {"billing_mode": "api"}
//! }}
//! ```

use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::OnceLock;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BillingConfig {
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderBilling>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProviderBilling {
    #[serde(default)]
    pub billing_mode: Option<String>,
    #[serde(default)]
    pub plan: Option<String>,
    #[serde(default)]
    pub monthly_fee_usd: Option<f64>,
    #[serde(default)]
    pub windows: Vec<WindowShape>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WindowShape {
    pub name: String,
    pub minutes: i64,
}

impl BillingConfig {
    pub fn mode(&self, provider: &str) -> &str {
        self.providers
            .get(provider)
            .and_then(|billing| billing.billing_mode.as_deref())
            .unwrap_or("unknown")
    }

    /// Window shapes for providers without quota telemetry.
    pub fn windows(&self, provider: &str) -> Vec<WindowShape> {
        if let Some(billing) = self.providers.get(provider)
            && !billing.windows.is_empty()
        {
            return billing.windows.clone();
        }
        match provider {
            "claude" => vec![
                WindowShape {
                    name: "5h".into(),
                    minutes: 300,
                },
                WindowShape {
                    name: "weekly".into(),
                    minutes: 10_080,
                },
            ],
            "cursor" => vec![WindowShape {
                name: "month".into(),
                minutes: 43_200,
            }],
            _ => Vec::new(),
        }
    }
}

pub fn config() -> &'static BillingConfig {
    static CONFIG: OnceLock<BillingConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        crate::data_home::directory()
            .ok()
            .and_then(|dir| std::fs::read_to_string(dir.join("billing.json")).ok())
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default()
    })
}
