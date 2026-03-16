use std::collections::{HashMap, HashSet};

use serde::Deserialize;

use crate::kind::asset::{AssetSpec, DesiredCondition, DesiredSetEntry, SourceRef};
use crate::kind::origin::OriginSpec;
use crate::kind::source::SourceSpec;
use crate::kind::sync::{StepType, SyncSpec, SyncStep};
use crate::kind::{Metadata, NagiKind, API_VERSION};

/// Minimal representation of dbt's `manifest.json`.
/// Only the fields Nagi needs are deserialized; the rest are ignored.
#[derive(Debug, Deserialize)]
pub struct DbtManifest {
    pub nodes: HashMap<String, DbtNode>,
    #[serde(default)]
    pub sources: HashMap<String, DbtSource>,
}

#[derive(Debug, Deserialize)]
pub struct DbtNode {
    pub unique_id: String,
    pub resource_type: String,
    pub name: String,
    pub package_name: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub depends_on: DbtDependsOn,
    #[serde(default)]
    pub test_metadata: Option<DbtTestMetadata>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DbtDependsOn {
    #[serde(default)]
    pub nodes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct DbtTestMetadata {
    pub name: String,
    #[serde(default)]
    pub kwargs: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct DbtSource {
    pub unique_id: String,
    pub name: String,
    pub source_name: String,
}

/// Converts a parsed dbt manifest into Nagi resources.
pub fn manifest_to_resources(manifest: &DbtManifest, origin: &OriginSpec) -> Vec<NagiKind> {
    let OriginSpec::DBT {
        connection,
        default_sync,
        ..
    } = origin;

    let mut resources: Vec<NagiKind> = Vec::new();

    // Collect all source unique_ids to map them to Nagi Source names.
    let mut source_map: HashMap<String, String> = HashMap::new();
    let mut seen_source_names: HashSet<String> = HashSet::new();
    for source in manifest.sources.values() {
        // Use "source_name.name" as the Nagi Source name to avoid collisions.
        let nagi_name = format!("{}.{}", source.source_name, source.name);
        source_map.insert(source.unique_id.clone(), nagi_name.clone());
        if seen_source_names.insert(nagi_name.clone()) {
            resources.push(NagiKind::Source {
                api_version: API_VERSION.to_string(),
                metadata: Metadata { name: nagi_name },
                spec: SourceSpec {
                    connection: connection.clone(),
                },
            });
        }
    }

    // Single pass: build model lookup and collect model nodes.
    let mut model_names: HashMap<String, String> = HashMap::new();
    let mut model_nodes: Vec<&DbtNode> = Vec::new();
    for node in manifest.nodes.values() {
        if node.resource_type == "model" {
            model_names.insert(node.unique_id.clone(), node.name.clone());
            model_nodes.push(node);
        }
    }
    model_nodes.sort_by_key(|n| &n.name);

    // Collect tests grouped by the model they depend on.
    let mut model_tests: HashMap<String, Vec<&DbtNode>> = HashMap::new();
    for node in manifest.nodes.values() {
        if node.resource_type == "test" {
            for dep in &node.depends_on.nodes {
                if model_names.contains_key(dep) {
                    model_tests.entry(dep.clone()).or_default().push(node);
                }
            }
        }
    }

    // Collect all tags used across models for Sync generation.
    let mut all_tag_sets: Vec<Vec<String>> = Vec::new();

    for model in &model_nodes {
        let mut sources_refs = Vec::new();
        let mut model_deps = Vec::new();

        for dep_id in &model.depends_on.nodes {
            if let Some(source_name) = source_map.get(dep_id) {
                sources_refs.push(SourceRef {
                    ref_name: source_name.clone(),
                });
            } else if let Some(model_name) = model_names.get(dep_id) {
                model_deps.push(model_name.clone());
            }
        }

        // Convert dbt tests to desiredSets.
        let desired_sets = if let Some(tests) = model_tests.get(&model.unique_id) {
            tests_to_desired_sets(tests)
        } else {
            Vec::new()
        };

        // Collect tags for Sync generation.
        if !model.tags.is_empty() {
            let mut sorted = model.tags.clone();
            sorted.sort();
            all_tag_sets.push(sorted);
        }

        resources.push(NagiKind::Asset {
            api_version: API_VERSION.to_string(),
            metadata: Metadata {
                name: model.name.clone(),
            },
            spec: AssetSpec {
                tags: model.tags.clone(),
                sources: sources_refs,
                desired_sets,
                auto_sync: true,
                sync: default_sync.clone(),
                resync: None,
            },
        });
    }

    // Generate tag-based Syncs.
    let tag_syncs = generate_tag_syncs(&all_tag_sets);
    resources.extend(tag_syncs);

    resources
}

/// Converts dbt tests into Nagi DesiredCondition entries.
/// All tests are executed via `dbt test --select` to ensure behavior matches
/// dbt's own test implementation exactly.
fn tests_to_desired_sets(tests: &[&DbtNode]) -> Vec<DesiredSetEntry> {
    let mut entries = Vec::new();
    for test in tests {
        if test.test_metadata.is_some() {
            entries.push(DesiredSetEntry::Inline(DesiredCondition::Command {
                name: format!("dbt-test-{}", test.name),
                run: vec![
                    "dbt".to_string(),
                    "test".to_string(),
                    "--select".to_string(),
                    test.name.clone(),
                ],
            }));
        }
    }
    entries
}

/// Generates `kind: Sync` resources for each unique tag and each realized tag combination.
fn generate_tag_syncs(tag_sets: &[Vec<String>]) -> Vec<NagiKind> {
    let mut single_tags: HashSet<String> = HashSet::new();
    let mut combo_tags: HashSet<Vec<String>> = HashSet::new();

    for tags in tag_sets {
        for tag in tags {
            single_tags.insert(tag.clone());
        }
        if tags.len() > 1 {
            combo_tags.insert(tags.clone());
        }
    }

    let mut syncs = Vec::new();

    let mut sorted_singles: Vec<String> = single_tags.into_iter().collect();
    sorted_singles.sort();

    for tag in &sorted_singles {
        let sync_name = format!("dbt-tag-{tag}");
        let selector = format!("tag:{tag}");
        syncs.push(make_tag_sync(&sync_name, &selector));
    }

    let mut sorted_combos: Vec<Vec<String>> = combo_tags.into_iter().collect();
    sorted_combos.sort();

    for tags in &sorted_combos {
        let sync_name = format!("dbt-tag-{}", tags.join("-"));
        let selector = tags
            .iter()
            .map(|t| format!("tag:{t}"))
            .collect::<Vec<_>>()
            .join(",");
        syncs.push(make_tag_sync(&sync_name, &selector));
    }

    syncs
}

fn make_tag_sync(name: &str, selector: &str) -> NagiKind {
    NagiKind::Sync {
        api_version: API_VERSION.to_string(),
        metadata: Metadata {
            name: name.to_string(),
        },
        spec: SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec![
                    "dbt".to_string(),
                    "run".to_string(),
                    "--select".to_string(),
                    selector.to_string(),
                ],
            },
            post: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kind::asset::SyncRef;

    /// A simplified jaffle_shop manifest for testing.
    fn jaffle_shop_manifest_json() -> &'static str {
        r#"{
  "nodes": {
    "model.jaffle_shop.stg_customers": {
      "unique_id": "model.jaffle_shop.stg_customers",
      "resource_type": "model",
      "name": "stg_customers",
      "package_name": "jaffle_shop",
      "tags": [],
      "depends_on": {
        "nodes": ["source.jaffle_shop.raw.customers"]
      }
    },
    "model.jaffle_shop.stg_orders": {
      "unique_id": "model.jaffle_shop.stg_orders",
      "resource_type": "model",
      "name": "stg_orders",
      "package_name": "jaffle_shop",
      "tags": [],
      "depends_on": {
        "nodes": ["source.jaffle_shop.raw.orders"]
      }
    },
    "model.jaffle_shop.customers": {
      "unique_id": "model.jaffle_shop.customers",
      "resource_type": "model",
      "name": "customers",
      "package_name": "jaffle_shop",
      "tags": ["finance"],
      "depends_on": {
        "nodes": [
          "model.jaffle_shop.stg_customers",
          "model.jaffle_shop.stg_orders"
        ]
      }
    },
    "model.jaffle_shop.orders": {
      "unique_id": "model.jaffle_shop.orders",
      "resource_type": "model",
      "name": "orders",
      "package_name": "jaffle_shop",
      "tags": ["finance", "daily"],
      "depends_on": {
        "nodes": [
          "model.jaffle_shop.stg_orders"
        ]
      }
    },
    "test.jaffle_shop.not_null_customers_customer_id.abc123": {
      "unique_id": "test.jaffle_shop.not_null_customers_customer_id.abc123",
      "resource_type": "test",
      "name": "not_null_customers_customer_id",
      "package_name": "jaffle_shop",
      "tags": [],
      "depends_on": {
        "nodes": ["model.jaffle_shop.customers"]
      },
      "test_metadata": {
        "name": "not_null",
        "kwargs": {
          "column_name": "customer_id",
          "model": "ref('customers')"
        }
      }
    },
    "test.jaffle_shop.unique_customers_customer_id.def456": {
      "unique_id": "test.jaffle_shop.unique_customers_customer_id.def456",
      "resource_type": "test",
      "name": "unique_customers_customer_id",
      "package_name": "jaffle_shop",
      "tags": [],
      "depends_on": {
        "nodes": ["model.jaffle_shop.customers"]
      },
      "test_metadata": {
        "name": "unique",
        "kwargs": {
          "column_name": "customer_id",
          "model": "ref('customers')"
        }
      }
    },
    "test.jaffle_shop.accepted_values_orders_status.ghi789": {
      "unique_id": "test.jaffle_shop.accepted_values_orders_status.ghi789",
      "resource_type": "test",
      "name": "accepted_values_orders_status",
      "package_name": "jaffle_shop",
      "tags": [],
      "depends_on": {
        "nodes": ["model.jaffle_shop.orders"]
      },
      "test_metadata": {
        "name": "accepted_values",
        "kwargs": {
          "column_name": "status",
          "values": ["placed", "shipped", "completed", "returned"]
        }
      }
    },
    "seed.jaffle_shop.raw_customers": {
      "unique_id": "seed.jaffle_shop.raw_customers",
      "resource_type": "seed",
      "name": "raw_customers",
      "package_name": "jaffle_shop",
      "tags": [],
      "depends_on": {
        "nodes": []
      }
    }
  },
  "sources": {
    "source.jaffle_shop.raw.customers": {
      "unique_id": "source.jaffle_shop.raw.customers",
      "name": "customers",
      "source_name": "raw"
    },
    "source.jaffle_shop.raw.orders": {
      "unique_id": "source.jaffle_shop.raw.orders",
      "name": "orders",
      "source_name": "raw"
    }
  }
}"#
    }

    fn jaffle_shop_origin() -> OriginSpec {
        OriginSpec::DBT {
            connection: "my-bigquery".to_string(),
            project_dir: "../dbt-project".to_string(),
            default_sync: Some(SyncRef {
                ref_name: "dbt-default".to_string(),
                with: HashMap::new(),
            }),
        }
    }

    #[test]
    fn parse_manifest_json() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let models: Vec<_> = manifest
            .nodes
            .values()
            .filter(|n| n.resource_type == "model")
            .collect();
        assert_eq!(models.len(), 4);
        assert_eq!(manifest.sources.len(), 2);
    }

    #[test]
    fn manifest_to_resources_generates_sources() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let sources: Vec<_> = resources.iter().filter(|r| r.kind() == "Source").collect();
        assert_eq!(sources.len(), 2);

        let source_names: HashSet<_> = sources.iter().map(|s| s.metadata().name.as_str()).collect();
        assert!(source_names.contains("raw.customers"));
        assert!(source_names.contains("raw.orders"));
    }

    #[test]
    fn manifest_to_resources_generates_assets() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let assets: Vec<_> = resources.iter().filter(|r| r.kind() == "Asset").collect();
        assert_eq!(assets.len(), 4);

        let asset_names: Vec<_> = assets.iter().map(|a| a.metadata().name.as_str()).collect();
        assert!(asset_names.contains(&"customers"));
        assert!(asset_names.contains(&"orders"));
        assert!(asset_names.contains(&"stg_customers"));
        assert!(asset_names.contains(&"stg_orders"));
    }

    #[test]
    fn manifest_to_resources_maps_tags() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let orders = resources
            .iter()
            .find(|r| r.metadata().name == "orders")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = orders {
            assert!(spec.tags.contains(&"finance".to_string()));
            assert!(spec.tags.contains(&"daily".to_string()));
        } else {
            panic!("orders should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_resolves_source_deps() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let stg_customers = resources
            .iter()
            .find(|r| r.metadata().name == "stg_customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = stg_customers {
            assert_eq!(spec.sources.len(), 1);
            assert_eq!(spec.sources[0].ref_name, "raw.customers");
        } else {
            panic!("stg_customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_applies_default_sync() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert_eq!(spec.sync.as_ref().unwrap().ref_name, "dbt-default");
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_maps_not_null_test() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            let has_not_null = spec.desired_sets.iter().any(|e| {
                matches!(e, DesiredSetEntry::Inline(DesiredCondition::Command { name, run, .. })
                    if name == "dbt-test-not_null_customers_customer_id"
                    && run == &["dbt", "test", "--select", "not_null_customers_customer_id"])
            });
            assert!(
                has_not_null,
                "customers should have a not_null Command condition"
            );
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_maps_unique_test() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            let has_unique = spec.desired_sets.iter().any(|e| {
                matches!(e, DesiredSetEntry::Inline(DesiredCondition::Command { name, run, .. })
                    if name == "dbt-test-unique_customers_customer_id"
                    && run == &["dbt", "test", "--select", "unique_customers_customer_id"])
            });
            assert!(has_unique, "customers should have a unique Command condition");
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_maps_generic_test_as_command() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let orders = resources
            .iter()
            .find(|r| r.metadata().name == "orders")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = orders {
            let has_command = spec.desired_sets.iter().any(|e| {
                matches!(e, DesiredSetEntry::Inline(DesiredCondition::Command { run, .. })
                    if run.contains(&"dbt".to_string()))
            });
            assert!(
                has_command,
                "orders should have a dbt test Command condition"
            );
        } else {
            panic!("orders should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_generates_single_tag_syncs() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let syncs: Vec<_> = resources.iter().filter(|r| r.kind() == "Sync").collect();

        let sync_names: Vec<_> = syncs.iter().map(|s| s.metadata().name.as_str()).collect();
        assert!(
            sync_names.contains(&"dbt-tag-finance"),
            "missing dbt-tag-finance"
        );
        assert!(
            sync_names.contains(&"dbt-tag-daily"),
            "missing dbt-tag-daily"
        );
    }

    #[test]
    fn manifest_to_resources_generates_combo_tag_syncs() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let syncs: Vec<_> = resources.iter().filter(|r| r.kind() == "Sync").collect();

        let sync_names: Vec<_> = syncs.iter().map(|s| s.metadata().name.as_str()).collect();
        assert!(
            sync_names.contains(&"dbt-tag-daily-finance"),
            "missing combo tag sync dbt-tag-daily-finance"
        );
    }

    #[test]
    fn manifest_to_resources_no_default_sync() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let origin = OriginSpec::DBT {
            connection: "my-bq".to_string(),
            project_dir: "../dbt-project".to_string(),
            default_sync: None,
        };
        let resources = manifest_to_resources(&manifest, &origin);

        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert!(spec.sync.is_none());
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_ignores_seeds() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let has_seed = resources
            .iter()
            .any(|r| r.metadata().name == "raw_customers" && r.kind() == "Asset");
        assert!(!has_seed, "seeds should not be converted to Assets");
    }

    #[test]
    fn manifest_to_resources_source_connection_matches_origin() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let source = resources.iter().find(|r| r.kind() == "Source").unwrap();
        if let NagiKind::Source { spec, .. } = source {
            assert_eq!(spec.connection, "my-bigquery");
        } else {
            panic!("expected Source");
        }
    }

    #[test]
    fn tag_sync_has_correct_dbt_run_command() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let sync = resources
            .iter()
            .find(|r| r.metadata().name == "dbt-tag-finance")
            .unwrap();
        if let NagiKind::Sync { spec, .. } = sync {
            assert_eq!(spec.run.args, vec!["dbt", "run", "--select", "tag:finance"]);
        } else {
            panic!("expected Sync");
        }
    }
}
