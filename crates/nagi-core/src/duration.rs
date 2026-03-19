use std::time::Duration as StdDuration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A duration parsed from a human-readable string (e.g. "24h", "30m", "1h30m").
/// Uses `humantime` format. Preserves the original string for serialization.
#[derive(Debug, Clone)]
pub struct Duration {
    inner: StdDuration,
    raw: String,
}

impl Duration {
    pub fn as_std(&self) -> StdDuration {
        self.inner
    }
}

impl PartialEq for Duration {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let inner = humantime::parse_duration(&s).map_err(serde::de::Error::custom)?;
        Ok(Duration { inner, raw: s })
    }
}

impl Serialize for Duration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.raw)
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

    macro_rules! parse_duration_test {
        ($($name:ident: $input:expr => $secs:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let w: Wrapper = serde_yaml::from_str($input).unwrap();
                    assert_eq!(w.d.as_std(), StdDuration::from_secs($secs));
                }
            )*
        };
    }

    parse_duration_test! {
        parse_hours: "d: 24h" => 24 * 3600;
        parse_minutes: "d: 30m" => 30 * 60;
        parse_compound: "d: 1h 30m" => 90 * 60;
    }

    #[test]
    fn rejects_invalid_string() {
        let result: Result<Wrapper, _> = serde_yaml::from_str("d: invalid");
        assert!(result.is_err());
    }

    #[test]
    fn serialize_roundtrip() {
        let w: Wrapper = serde_yaml::from_str("d: 24h").unwrap();
        let yaml = serde_yaml::to_string(&w).unwrap();
        assert!(yaml.contains("24h"));
        let w2: Wrapper = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(w.d, w2.d);
    }
}
