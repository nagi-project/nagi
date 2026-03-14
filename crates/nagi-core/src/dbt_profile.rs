use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbtProfileError {
    #[error("profiles.yml not found at {path}")]
    NotFound { path: PathBuf },
    #[error("failed to read profiles.yml: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse profiles.yml: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("profile '{profile}' not found in profiles.yml")]
    ProfileNotFound { profile: String },
    #[error("target '{target}' not found in profile '{profile}'")]
    TargetNotFound { profile: String, target: String },
}

/// Parsed representation of `~/.dbt/profiles.yml`.
#[derive(Debug, Clone, PartialEq)]
pub struct DbtProfilesFile {
    profiles: HashMap<String, Profile>,
}

/// A single dbt profile entry with its outputs.
#[derive(Debug, Clone, PartialEq)]
pub struct Profile {
    pub default_target: String,
    pub outputs: HashMap<String, OutputConfig>,
}

/// Connection configuration for a single target output.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OutputConfig {
    #[serde(rename = "type")]
    pub adapter_type: String,
    #[serde(flatten)]
    pub fields: HashMap<String, serde_yaml::Value>,
}

// Raw deserialization types — not exposed publicly.
#[derive(Deserialize)]
struct RawProfile {
    target: String,
    outputs: HashMap<String, OutputConfig>,
}

impl DbtProfilesFile {
    /// Reads and parses `profiles.yml` from the given path.
    pub fn load(path: &Path) -> Result<Self, DbtProfileError> {
        if !path.exists() {
            return Err(DbtProfileError::NotFound {
                path: path.to_owned(),
            });
        }
        let content = std::fs::read_to_string(path)?;
        Self::parse_str(&content)
    }

    /// Loads from the default location: `~/.dbt/profiles.yml`.
    pub fn load_default() -> Result<Self, DbtProfileError> {
        let path = default_profiles_path();
        Self::load(&path)
    }

    pub fn parse_str(content: &str) -> Result<Self, DbtProfileError> {
        let raw: HashMap<String, RawProfile> = serde_yaml::from_str(content)?;
        let profiles = raw
            .into_iter()
            .map(|(name, raw)| {
                (
                    name,
                    Profile {
                        default_target: raw.target,
                        outputs: raw.outputs,
                    },
                )
            })
            .collect();
        Ok(Self { profiles })
    }

    /// Returns the profile names in sorted order.
    pub fn profile_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.profiles.keys().map(|s| s.as_str()).collect();
        names.sort();
        names
    }

    /// Resolves the output config for the given profile and optional target.
    /// Falls back to the profile's default target when `target` is `None`.
    pub fn resolve(
        &self,
        profile: &str,
        target: Option<&str>,
    ) -> Result<&OutputConfig, DbtProfileError> {
        let p = self
            .profiles
            .get(profile)
            .ok_or_else(|| DbtProfileError::ProfileNotFound {
                profile: profile.to_string(),
            })?;
        let target_name = target.unwrap_or(&p.default_target);
        p.outputs
            .get(target_name)
            .ok_or_else(|| DbtProfileError::TargetNotFound {
                profile: profile.to_string(),
                target: target_name.to_string(),
            })
    }
}

fn default_profiles_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".dbt")
        .join("profiles.yml")
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILES_YAML: &str = r#"
my_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      project: my-gcp-project
      dataset: raw
      threads: 4
    prod:
      type: bigquery
      project: my-gcp-project-prod
      dataset: prod
      threads: 8
other_project:
  target: staging
  outputs:
    staging:
      type: bigquery
      project: other-gcp-project
      dataset: staging
"#;

    #[test]
    fn parse_profile_names() {
        let f = DbtProfilesFile::parse_str(PROFILES_YAML).unwrap();
        assert_eq!(f.profile_names(), vec!["my_project", "other_project"]);
    }

    #[test]
    fn resolve_explicit_target() {
        let f = DbtProfilesFile::parse_str(PROFILES_YAML).unwrap();
        let out = f.resolve("my_project", Some("prod")).unwrap();
        assert_eq!(out.adapter_type, "bigquery");
        assert_eq!(
            out.fields.get("project").unwrap(),
            &serde_yaml::Value::String("my-gcp-project-prod".to_string())
        );
    }

    #[test]
    fn resolve_default_target() {
        let f = DbtProfilesFile::parse_str(PROFILES_YAML).unwrap();
        let out = f.resolve("my_project", None).unwrap();
        assert_eq!(out.adapter_type, "bigquery");
        assert_eq!(
            out.fields.get("project").unwrap(),
            &serde_yaml::Value::String("my-gcp-project".to_string())
        );
    }

    #[test]
    fn rejects_unknown_profile() {
        let f = DbtProfilesFile::parse_str(PROFILES_YAML).unwrap();
        let err = f.resolve("no_such_profile", None).unwrap_err();
        assert!(
            matches!(err, DbtProfileError::ProfileNotFound { profile } if profile == "no_such_profile")
        );
    }

    #[test]
    fn rejects_unknown_target() {
        let f = DbtProfilesFile::parse_str(PROFILES_YAML).unwrap();
        let err = f.resolve("my_project", Some("no_such_target")).unwrap_err();
        assert!(matches!(
            err,
            DbtProfileError::TargetNotFound { profile, target }
            if profile == "my_project" && target == "no_such_target"
        ));
    }

    #[test]
    fn rejects_invalid_yaml() {
        let result = DbtProfilesFile::parse_str("{ invalid: yaml: here }");
        assert!(result.is_err());
    }
}
