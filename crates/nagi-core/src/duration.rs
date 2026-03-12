use std::time::Duration as StdDuration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A duration parsed from a human-readable string (e.g. "24h", "30m", "1h30m").
/// Uses `humantime` format. Fails at deserialization time if the string is invalid.
#[derive(Debug, Clone, PartialEq)]
pub struct Duration(StdDuration);

impl Duration {
    pub fn as_std(&self) -> StdDuration {
        self.0
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s)
            .map(Duration)
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for Duration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&humantime::format_duration(self.0).to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration as StdDuration;

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Deserialize, Serialize)]
    struct Wrapper {
        d: Duration,
    }

    #[test]
    fn parse_hours() {
        let w: Wrapper = serde_yaml::from_str("d: 24h").unwrap();
        assert_eq!(w.d.as_std(), StdDuration::from_secs(24 * 3600));
    }

    #[test]
    fn parse_minutes() {
        let w: Wrapper = serde_yaml::from_str("d: 30m").unwrap();
        assert_eq!(w.d.as_std(), StdDuration::from_secs(30 * 60));
    }

    #[test]
    fn parse_compound() {
        let w: Wrapper = serde_yaml::from_str("d: 1h 30m").unwrap();
        assert_eq!(w.d.as_std(), StdDuration::from_secs(90 * 60));
    }

    #[test]
    fn rejects_invalid_string() {
        let result: Result<Wrapper, _> = serde_yaml::from_str("d: invalid");
        assert!(result.is_err());
    }

    #[test]
    fn serialize_roundtrip() {
        let w = Wrapper {
            d: Duration(StdDuration::from_secs(24 * 3600)),
        };
        let yaml = serde_yaml::to_string(&w).unwrap();
        let w2: Wrapper = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(w.d, w2.d);
    }
}
