//! Cost and token bands for grouping. Token bands use fresh tokens (fresh
//! input + cache writes + output): cache reads dominate raw totals and would
//! put every agentic session in the top band.

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CostBand {
    Under10c,
    From10cTo1,
    From1To5,
    From5To20,
    Over20,
    Unpriced,
}

impl CostBand {
    pub const ALL: [CostBand; 6] = [
        Self::Over20,
        Self::From5To20,
        Self::From1To5,
        Self::From10cTo1,
        Self::Under10c,
        Self::Unpriced,
    ];

    pub fn of(cost: Option<f64>) -> Self {
        match cost {
            None => Self::Unpriced,
            Some(cost) if cost < 0.10 => Self::Under10c,
            Some(cost) if cost < 1.0 => Self::From10cTo1,
            Some(cost) if cost < 5.0 => Self::From1To5,
            Some(cost) if cost < 20.0 => Self::From5To20,
            Some(_) => Self::Over20,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Under10c => "<$0.10",
            Self::From10cTo1 => "$0.10–1",
            Self::From1To5 => "$1–5",
            Self::From5To20 => "$5–20",
            Self::Over20 => ">$20",
            Self::Unpriced => "unpriced",
        }
    }

    pub fn order_of_label(label: &str) -> usize {
        Self::ALL
            .iter()
            .position(|band| band.label() == label)
            .unwrap_or(usize::MAX)
    }

    pub(crate) fn sql(self, column: &str) -> String {
        match self {
            Self::Unpriced => format!("{column} IS NULL"),
            Self::Under10c => format!("{column} < 0.10"),
            Self::From10cTo1 => format!("{column} >= 0.10 AND {column} < 1.0"),
            Self::From1To5 => format!("{column} >= 1.0 AND {column} < 5.0"),
            Self::From5To20 => format!("{column} >= 5.0 AND {column} < 20.0"),
            Self::Over20 => format!("{column} >= 20.0"),
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|band| band.label() == value)
            .or(match value {
                "unpriced" => Some(Self::Unpriced),
                "lt0.1" => Some(Self::Under10c),
                "0.1-1" => Some(Self::From10cTo1),
                "1-5" => Some(Self::From1To5),
                "5-20" => Some(Self::From5To20),
                "gt20" => Some(Self::Over20),
                _ => None,
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TokenBand {
    Under100k,
    From100kTo1m,
    From1mTo5m,
    From5mTo20m,
    Over20m,
}

impl TokenBand {
    pub const ALL: [TokenBand; 5] = [
        Self::Over20m,
        Self::From5mTo20m,
        Self::From1mTo5m,
        Self::From100kTo1m,
        Self::Under100k,
    ];

    pub fn of(fresh_tokens: i64) -> Self {
        match fresh_tokens {
            t if t < 100_000 => Self::Under100k,
            t if t < 1_000_000 => Self::From100kTo1m,
            t if t < 5_000_000 => Self::From1mTo5m,
            t if t < 20_000_000 => Self::From5mTo20m,
            _ => Self::Over20m,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Under100k => "<100k",
            Self::From100kTo1m => "100k–1M",
            Self::From1mTo5m => "1–5M",
            Self::From5mTo20m => "5–20M",
            Self::Over20m => ">20M",
        }
    }

    pub fn order_of_label(label: &str) -> usize {
        Self::ALL
            .iter()
            .position(|band| band.label() == label)
            .unwrap_or(usize::MAX)
    }

    pub(crate) fn sql(self, column: &str) -> String {
        match self {
            Self::Under100k => format!("{column} < 100000"),
            Self::From100kTo1m => format!("{column} >= 100000 AND {column} < 1000000"),
            Self::From1mTo5m => format!("{column} >= 1000000 AND {column} < 5000000"),
            Self::From5mTo20m => format!("{column} >= 5000000 AND {column} < 20000000"),
            Self::Over20m => format!("{column} >= 20000000"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_bands_boundaries() {
        assert_eq!(CostBand::of(None), CostBand::Unpriced);
        assert_eq!(CostBand::of(Some(0.0999)), CostBand::Under10c);
        assert_eq!(CostBand::of(Some(0.10)), CostBand::From10cTo1);
        assert_eq!(CostBand::of(Some(1.0)), CostBand::From1To5);
        assert_eq!(CostBand::of(Some(5.0)), CostBand::From5To20);
        assert_eq!(CostBand::of(Some(20.0)), CostBand::Over20);
        assert_eq!(TokenBand::of(99_999), TokenBand::Under100k);
        assert_eq!(TokenBand::of(20_000_000), TokenBand::Over20m);
    }
}
