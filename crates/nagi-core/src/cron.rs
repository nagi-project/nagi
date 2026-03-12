use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A validated cron expression in standard 5-field format (e.g. "0 3 * * *").
/// Fails at deserialization time if the expression is invalid.
#[derive(Debug, Clone, PartialEq)]
pub struct CronSchedule(String);

impl CronSchedule {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CronSchedule {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        croner::Cron::new(&s)
            .parse()
            .map(|_| CronSchedule(s))
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for CronSchedule {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Deserialize, Serialize)]
    struct Wrapper {
        cron: CronSchedule,
    }

    #[test]
    fn parse_valid_cron() {
        let w: Wrapper = serde_yaml::from_str("cron: \"0 3 * * *\"").unwrap();
        assert_eq!(w.cron.as_str(), "0 3 * * *");
    }

    #[test]
    fn rejects_invalid_cron() {
        let result: Result<Wrapper, _> = serde_yaml::from_str("cron: \"invalid\"");
        assert!(result.is_err());
    }

    #[test]
    fn rejects_out_of_range_field() {
        let result: Result<Wrapper, _> = serde_yaml::from_str("cron: \"60 3 * * *\"");
        assert!(result.is_err());
    }

    #[test]
    fn serialize_roundtrip() {
        let w = Wrapper {
            cron: CronSchedule("0 3 * * *".to_string()),
        };
        let yaml = serde_yaml::to_string(&w).unwrap();
        let w2: Wrapper = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(w.cron, w2.cron);
    }
}
