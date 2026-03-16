use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod asset;
pub mod connection;
pub mod desired_group;
pub mod origin;
pub mod source;
pub mod sync;

pub use asset::AssetSpec;
pub use connection::ConnectionSpec;
pub use desired_group::DesiredGroupSpec;
pub use origin::OriginSpec;
pub use source::SourceSpec;
pub use sync::SyncSpec;

pub const API_VERSION: &str = "nagi.io/v1alpha1";

#[derive(Debug, Error)]
pub enum KindError {
    #[error("failed to parse YAML: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("invalid spec for kind {kind}: {message}")]
    InvalidSpec { kind: String, message: String },

    #[error("unsupported apiVersion '{version}': expected '{expected}'")]
    UnsupportedApiVersion { version: String, expected: String },
}

/// Common metadata shared by all resource kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub name: String,
}

/// A Nagi resource. Dispatched by the `kind` field in YAML, following the Kubernetes CRD convention.
/// Includes `apiVersion` to ensure the same YAML works in CLI, `nagi serve`, and future k8s environments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum NagiKind {
    Connection {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: Metadata,
        spec: ConnectionSpec,
    },
    Source {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: Metadata,
        spec: SourceSpec,
    },
    Asset {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: Metadata,
        spec: AssetSpec,
    },
    DesiredGroup {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: Metadata,
        spec: DesiredGroupSpec,
    },
    Sync {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: Metadata,
        spec: SyncSpec,
    },
    Origin {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: Metadata,
        spec: OriginSpec,
    },
}

impl NagiKind {
    pub fn api_version(&self) -> &str {
        match self {
            NagiKind::Connection { api_version, .. } => api_version,
            NagiKind::Source { api_version, .. } => api_version,
            NagiKind::Asset { api_version, .. } => api_version,
            NagiKind::DesiredGroup { api_version, .. } => api_version,
            NagiKind::Sync { api_version, .. } => api_version,
            NagiKind::Origin { api_version, .. } => api_version,
        }
    }

    pub fn metadata(&self) -> &Metadata {
        match self {
            NagiKind::Connection { metadata, .. } => metadata,
            NagiKind::Source { metadata, .. } => metadata,
            NagiKind::Asset { metadata, .. } => metadata,
            NagiKind::DesiredGroup { metadata, .. } => metadata,
            NagiKind::Sync { metadata, .. } => metadata,
            NagiKind::Origin { metadata, .. } => metadata,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            NagiKind::Connection { .. } => connection::KIND,
            NagiKind::Source { .. } => source::KIND,
            NagiKind::Asset { .. } => asset::KIND,
            NagiKind::DesiredGroup { .. } => desired_group::KIND,
            NagiKind::Sync { .. } => sync::KIND,
            NagiKind::Origin { .. } => origin::KIND,
        }
    }

    pub fn validate(&self) -> Result<(), KindError> {
        let version = self.api_version();
        if version != API_VERSION {
            return Err(KindError::UnsupportedApiVersion {
                version: version.to_string(),
                expected: API_VERSION.to_string(),
            });
        }
        let name = &self.metadata().name;
        if name.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: self.kind().to_string(),
                message: "metadata.name must not be empty".to_string(),
            });
        }
        if name.contains('/') || name.contains('\\') || name.contains("..") {
            return Err(KindError::InvalidSpec {
                kind: self.kind().to_string(),
                message: "metadata.name must not contain path separators or '..'".to_string(),
            });
        }
        match self {
            NagiKind::Connection { spec, .. } => spec.validate(),
            NagiKind::Source { spec, .. } => spec.validate(),
            NagiKind::Asset { spec, .. } => spec.validate(),
            NagiKind::DesiredGroup { spec, .. } => spec.validate(),
            NagiKind::Sync { spec, .. } => spec.validate(),
            NagiKind::Origin { spec, .. } => spec.validate(),
        }
    }
}

pub fn parse_kind(yaml: &str) -> Result<NagiKind, KindError> {
    let kind: NagiKind = serde_yaml::from_str(yaml)?;
    kind.validate()?;
    Ok(kind)
}

/// Parses multiple resources from a single YAML string. Supports `---` document separators.
pub fn parse_kinds(yaml: &str) -> Result<Vec<NagiKind>, KindError> {
    let mut kinds = Vec::new();
    for document in serde_yaml::Deserializer::from_str(yaml) {
        let kind = NagiKind::deserialize(document)?;
        kind.validate()?;
        kinds.push(kind);
    }
    Ok(kinds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_connection_resource() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bigquery
spec:
  dbtProfile:
    profile: my_project
    target: dev
"#;
        let resource = parse_kind(yaml).unwrap();
        assert_eq!(resource.kind(), connection::KIND);
        assert_eq!(resource.metadata().name, "my-bigquery");
        assert!(matches!(
            &resource,
            NagiKind::Connection { spec, .. }
                if spec.dbt_profile.profile == "my_project"
                && spec.dbt_profile.target == Some("dev".to_string())
        ));
    }

    #[test]
    fn parse_source_resource() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: Source
metadata:
  name: raw-sales
spec:
  connection: my-bigquery
"#;
        let resource = parse_kind(yaml).unwrap();
        assert_eq!(resource.kind(), source::KIND);
        assert_eq!(resource.metadata().name, "raw-sales");
        assert!(matches!(
            &resource,
            NagiKind::Source { spec, .. } if spec.connection == "my-bigquery"
        ));
    }

    #[test]
    fn parse_asset_resource() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sources:
    - ref: raw-sales
  desiredSets:
    - name: data-freshness
      type: Freshness
      maxAge: 24h
      interval: 6h
  sync:
    ref: dbt-default
"#;
        let resource = parse_kind(yaml).unwrap();
        assert_eq!(resource.kind(), asset::KIND);
        assert_eq!(resource.metadata().name, "daily-sales");
    }

    #[test]
    fn parse_sync_resource() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-default
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
"#;
        let resource = parse_kind(yaml).unwrap();
        assert_eq!(resource.kind(), sync::KIND);
        assert_eq!(resource.metadata().name, "dbt-default");
    }

    #[test]
    fn parse_multiple_resources() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bigquery
spec:
  dbtProfile:
    profile: my_project
---
apiVersion: nagi.io/v1alpha1
kind: Source
metadata:
  name: raw-sales
spec:
  connection: my-bigquery
"#;
        let resources = parse_kinds(yaml).unwrap();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].kind(), connection::KIND);
        assert_eq!(resources[1].kind(), source::KIND);
    }

    #[test]
    fn parse_kind_rejects_empty_name() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: Source
metadata:
  name: ""
spec:
  connection: my-bigquery
"#;
        let err = parse_kind(yaml).unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { .. }));
    }

    #[test]
    fn parse_kind_rejects_path_traversal_in_name() {
        let cases = [
            ("../../etc/cron", "path traversal with .."),
            ("foo/bar", "forward slash"),
            ("name..evil", "double dot"),
        ];
        for (name, desc) in cases {
            let yaml = format!(
                r#"
apiVersion: nagi.io/v1alpha1
kind: Source
metadata:
  name: "{name}"
spec:
  connection: my-bq
"#
            );
            let err = parse_kind(&yaml).unwrap_err();
            assert!(
                matches!(err, KindError::InvalidSpec { .. }),
                "expected InvalidSpec for {desc} (name '{name}'), got {err:?}"
            );
        }
        // Backslash tested via direct construction to avoid YAML escape issues.
        let resource = NagiKind::Source {
            api_version: API_VERSION.to_string(),
            metadata: Metadata {
                name: "foo\\bar".to_string(),
            },
            spec: source::SourceSpec {
                connection: "my-bq".to_string(),
            },
        };
        let err = resource.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { .. }));
    }

    #[test]
    fn metadata_accessor_works_for_all_kinds() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: my-sync
spec:
  run:
    type: Command
    args: ["dbt", "run"]
"#;
        let resource = parse_kind(yaml).unwrap();
        assert_eq!(resource.metadata().name, "my-sync");
    }

    #[test]
    fn parse_kind_rejects_unsupported_api_version() {
        let yaml = r#"
apiVersion: nagi.io/v2
kind: Source
metadata:
  name: raw-sales
spec:
  connection: my-bq
"#;
        let err = parse_kind(yaml).unwrap_err();
        assert!(matches!(err, KindError::UnsupportedApiVersion { .. }));
    }

    #[test]
    fn parse_kind_rejects_missing_api_version() {
        let yaml = r#"
kind: Source
metadata:
  name: raw-sales
spec:
  connection: my-bq
"#;
        let err = parse_kind(yaml).unwrap_err();
        assert!(matches!(err, KindError::YamlParse(_)));
    }

    #[test]
    fn parse_origin_resource() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: my-dbt-project
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  defaultSync:
    ref: dbt-default
"#;
        let resource = parse_kind(yaml).unwrap();
        assert_eq!(resource.kind(), origin::KIND);
        assert_eq!(resource.metadata().name, "my-dbt-project");
    }
}
