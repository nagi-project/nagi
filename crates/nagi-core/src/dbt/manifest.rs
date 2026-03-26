use std::collections::{HashMap, HashSet};

use serde::Deserialize;

use crate::kind::asset::{AssetSpec, DesiredCondition, MergePosition, OnDriftEntry};
use crate::kind::origin::OriginSpec;
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

    let (dbt_source_map, dbt_source_names) = collect_dbt_source_names(manifest);
    let (model_names, model_nodes) = collect_models(manifest);
    let dbt_source_tests = collect_tests(manifest, &dbt_source_map);
    let model_tests = collect_tests(manifest, &model_names);

    let (mut resources, needs_skip_sync) = build_dbt_source_assets(
        &dbt_source_names,
        &dbt_source_map,
        &dbt_source_tests,
        connection,
    );
    if needs_skip_sync {
        resources.push(make_skip_sync());
    }
    let (model_assets, tag_sets) = build_model_assets(
        &model_nodes,
        &model_tests,
        &dbt_source_map,
        &model_names,
        connection,
        default_sync,
    );
    resources.extend(model_assets);
    resources.extend(generate_tag_syncs(&tag_sets));
    resources
}

const SKIP_SYNC_NAME: &str = "nagi-skip-sync";

/// Builds Assets and Conditions for dbt sources (external tables).
/// Returns the resources and whether `nagi-skip-sync` is needed.
fn build_dbt_source_assets(
    names: &[String],
    dbt_source_map: &HashMap<String, String>,
    dbt_source_tests: &HashMap<String, Vec<&DbtNode>>,
    connection: &str,
) -> (Vec<NagiKind>, bool) {
    let mut resources: Vec<NagiKind> = Vec::new();
    let mut skip_sync_needed = false;

    // Re-key tests by nagi Asset name for O(1) lookup.
    let mut tests_by_name: HashMap<&str, Vec<&DbtNode>> = HashMap::new();
    for (uid, tests) in dbt_source_tests {
        if let Some(nagi_name) = dbt_source_map.get(uid) {
            tests_by_name
                .entry(nagi_name.as_str())
                .or_default()
                .extend(tests.iter().copied());
        }
    }

    for name in names {
        let tests_for_this = tests_by_name.get(name.as_str());
        let has_tests = tests_for_this.is_some_and(|t| !t.is_empty());

        let sync_ref = if has_tests {
            skip_sync_needed = true;
            Some(SKIP_SYNC_NAME.to_string())
        } else {
            None
        };
        let (on_drift, conditions_resource) =
            build_on_drift(tests_for_this.filter(|t| !t.is_empty()), name, &sync_ref);

        if let Some(cond) = conditions_resource {
            resources.push(cond);
        }

        resources.push(NagiKind::Asset {
            api_version: API_VERSION.to_string(),
            metadata: Metadata { name: name.clone() },
            spec: AssetSpec {
                tags: vec![],
                connection: Some(connection.to_string()),
                upstreams: vec![],
                on_drift,
                auto_sync: true,
                evaluate_cache_ttl: None,
            },
        });
    }

    (resources, skip_sync_needed)
}

/// Generates the `nagi-skip-sync` Sync resource: runs `true` (exits 0 immediately).
fn make_skip_sync() -> NagiKind {
    NagiKind::Sync {
        api_version: API_VERSION.to_string(),
        metadata: Metadata {
            name: SKIP_SYNC_NAME.to_string(),
        },
        spec: SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["true".to_string()],
                env: HashMap::new(),
            },
            post: None,
        },
    }
}

/// Builds Assets and Conditions for dbt models.
/// Returns the resources and collected tag sets (for tag Sync generation by the caller).
fn build_model_assets(
    model_nodes: &[&DbtNode],
    model_tests: &HashMap<String, Vec<&DbtNode>>,
    dbt_source_map: &HashMap<String, String>,
    model_names: &HashMap<String, String>,
    connection: &str,
    default_sync: &Option<String>,
) -> (Vec<NagiKind>, Vec<Vec<String>>) {
    let mut resources: Vec<NagiKind> = Vec::new();
    let mut all_tag_sets: Vec<Vec<String>> = Vec::new();

    for model in model_nodes {
        let upstreams = resolve_upstreams(&model.depends_on.nodes, dbt_source_map, model_names);

        let (on_drift, conditions_resource) =
            build_on_drift(model_tests.get(&model.unique_id), &model.name, default_sync);

        if let Some(cond) = conditions_resource {
            resources.push(cond);
        }

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
                connection: Some(connection.to_string()),
                upstreams,
                on_drift,
                auto_sync: true,
                evaluate_cache_ttl: None,
            },
        });
    }

    (resources, all_tag_sets)
}

/// Builds a dbt source unique_id → nagi Asset name lookup and a deduplicated list of names.
fn collect_dbt_source_names(manifest: &DbtManifest) -> (HashMap<String, String>, Vec<String>) {
    let mut id_to_name: HashMap<String, String> = HashMap::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut names: Vec<String> = Vec::new();

    for dbt_src in manifest.sources.values() {
        let nagi_name = format!("{}.{}", dbt_src.source_name, dbt_src.name);
        id_to_name.insert(dbt_src.unique_id.clone(), nagi_name.clone());
        if seen.insert(nagi_name.clone()) {
            names.push(nagi_name);
        }
    }
    names.sort();
    (id_to_name, names)
}

/// Extracts model nodes sorted by name and builds a unique_id → name lookup.
fn collect_models(manifest: &DbtManifest) -> (HashMap<String, String>, Vec<&DbtNode>) {
    let mut model_names: HashMap<String, String> = HashMap::new();
    let mut model_nodes: Vec<&DbtNode> = Vec::new();
    for node in manifest.nodes.values() {
        if node.resource_type == "model" {
            model_names.insert(node.unique_id.clone(), node.name.clone());
            model_nodes.push(node);
        }
    }
    model_nodes.sort_by_key(|n| &n.name);
    (model_names, model_nodes)
}

/// Groups test nodes by the unique_id of the resource they depend on.
/// Only dependencies whose unique_id is in `valid_ids` are included.
fn collect_tests<'a>(
    manifest: &'a DbtManifest,
    valid_ids: &HashMap<String, String>,
) -> HashMap<String, Vec<&'a DbtNode>> {
    let mut tests: HashMap<String, Vec<&DbtNode>> = HashMap::new();
    for node in manifest.nodes.values() {
        if node.resource_type == "test" {
            for dep in &node.depends_on.nodes {
                if valid_ids.contains_key(dep) {
                    tests.entry(dep.clone()).or_default().push(node);
                }
            }
        }
    }
    tests
}

/// Resolves dbt dependency ids into upstream Asset names.
fn resolve_upstreams(
    dep_ids: &[String],
    dbt_source_map: &HashMap<String, String>,
    model_names: &HashMap<String, String>,
) -> Vec<String> {
    let mut upstreams = Vec::new();
    for dep_id in dep_ids {
        if let Some(name) = dbt_source_map.get(dep_id) {
            upstreams.push(name.clone());
        } else if let Some(model_name) = model_names.get(dep_id) {
            upstreams.push(model_name.clone());
        }
    }
    upstreams
}

/// Builds on_drift entries and an optional Conditions resource from model tests.
fn build_on_drift(
    tests: Option<&Vec<&DbtNode>>,
    model_name: &str,
    default_sync: &Option<String>,
) -> (Vec<OnDriftEntry>, Option<NagiKind>) {
    let conditions = tests
        .map(|t| tests_to_conditions(t))
        .filter(|c| !c.is_empty());

    let Some(conditions) = conditions else {
        return (Vec::new(), None);
    };

    let group_name = format!("dbt-tests-{model_name}");
    let conditions_resource = NagiKind::Conditions {
        api_version: API_VERSION.to_string(),
        metadata: Metadata {
            name: group_name.clone(),
        },
        spec: crate::kind::condition::ConditionsSpec(conditions),
    };

    let on_drift = default_sync
        .as_ref()
        .map(|sync_name| OnDriftEntry {
            conditions: group_name,
            sync: sync_name.clone(),
            with: HashMap::new(),
            merge_position: MergePosition::BeforeOrigin,
        })
        .into_iter()
        .collect();

    (on_drift, Some(conditions_resource))
}

/// Converts dbt tests into Nagi DesiredCondition entries.
/// All tests are executed via `dbt test --select` to ensure behavior matches
/// dbt's own test implementation exactly.
fn tests_to_conditions(tests: &[&DbtNode]) -> Vec<DesiredCondition> {
    let mut entries = Vec::new();
    for test in tests {
        if test.test_metadata.is_some() {
            entries.push(DesiredCondition::Command {
                name: format!("dbt-test-{}", test.name),
                run: vec![
                    "dbt".to_string(),
                    "test".to_string(),
                    "--select".to_string(),
                    test.name.clone(),
                ],
                interval: None,
                env: HashMap::new(),
                evaluate_cache_ttl: None,
            });
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
                env: HashMap::new(),
            },
            post: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    "test.jaffle_shop.not_null_raw_orders_order_id.src123": {
      "unique_id": "test.jaffle_shop.not_null_raw_orders_order_id.src123",
      "resource_type": "test",
      "name": "not_null_raw_orders_order_id",
      "package_name": "jaffle_shop",
      "tags": [],
      "depends_on": {
        "nodes": ["source.jaffle_shop.raw.orders"]
      },
      "test_metadata": {
        "name": "not_null",
        "kwargs": {
          "column_name": "order_id",
          "model": "source('raw', 'orders')"
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
            default_sync: Some("dbt-default".to_string()),
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
    fn manifest_to_resources_generates_assets_for_sources_and_models() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let assets: Vec<_> = resources.iter().filter(|r| r.kind() == "Asset").collect();
        // 2 dbt source Assets + 4 model Assets
        assert_eq!(assets.len(), 6);

        let asset_names: HashSet<_> = assets.iter().map(|a| a.metadata().name.as_str()).collect();
        assert!(asset_names.contains("raw.customers"));
        assert!(asset_names.contains("raw.orders"));
        assert!(asset_names.contains("customers"));
        assert!(asset_names.contains("orders"));
        assert!(asset_names.contains("stg_customers"));
        assert!(asset_names.contains("stg_orders"));
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
    fn manifest_to_resources_resolves_upstreams() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        // stg_customers depends on raw.customers
        let stg_customers = resources
            .iter()
            .find(|r| r.metadata().name == "stg_customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = stg_customers {
            assert_eq!(spec.upstreams, vec!["raw.customers"]);
        } else {
            panic!("stg_customers should be an Asset");
        }

        // customers depends on model stg_customers and stg_orders
        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert!(spec.upstreams.contains(&"stg_customers".to_string()));
            assert!(spec.upstreams.contains(&"stg_orders".to_string()));
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_applies_default_sync_via_on_drift() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert_eq!(spec.on_drift.len(), 1);
            assert_eq!(spec.on_drift[0].sync, "dbt-default");
            assert_eq!(spec.on_drift[0].conditions, "dbt-tests-customers");
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_applies_dbt_source_tests() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        // raw.orders has a not_null test → should have on_drift with nagi-skip-sync
        let raw_orders = resources
            .iter()
            .find(|r| r.metadata().name == "raw.orders")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = raw_orders {
            assert_eq!(spec.on_drift.len(), 1);
            assert_eq!(spec.on_drift[0].sync, SKIP_SYNC_NAME);
            assert!(spec.auto_sync);
        } else {
            panic!("raw.orders should be an Asset");
        }

        // raw.customers has no tests → on_drift should be empty
        let raw_customers = resources
            .iter()
            .find(|r| r.metadata().name == "raw.customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = raw_customers {
            assert!(spec.on_drift.is_empty());
        } else {
            panic!("raw.customers should be an Asset");
        }

        // nagi-skip-sync should be generated
        let noop = resources
            .iter()
            .find(|r| r.metadata().name == SKIP_SYNC_NAME);
        assert!(noop.is_some(), "nagi-skip-sync should be generated");
    }

    #[test]
    fn manifest_to_resources_generates_conditions_for_tests() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let group = resources
            .iter()
            .find(|r| r.metadata().name == "dbt-tests-customers")
            .unwrap();
        if let NagiKind::Conditions { spec, .. } = group {
            let has_not_null = spec.0.iter().any(|c| {
                matches!(c, DesiredCondition::Command { name, run, .. }
                    if name == "dbt-test-not_null_customers_customer_id"
                    && run == &["dbt", "test", "--select", "not_null_customers_customer_id"])
            });
            assert!(has_not_null, "should have a not_null Command condition");

            let has_unique = spec.0.iter().any(|c| {
                matches!(c, DesiredCondition::Command { name, run, .. }
                    if name == "dbt-test-unique_customers_customer_id"
                    && run == &["dbt", "test", "--select", "unique_customers_customer_id"])
            });
            assert!(has_unique, "should have a unique Command condition");
        } else {
            panic!("expected Conditions");
        }
    }

    #[test]
    fn manifest_to_resources_maps_generic_test_as_command() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        let group = resources
            .iter()
            .find(|r| r.metadata().name == "dbt-tests-orders")
            .unwrap();
        if let NagiKind::Conditions { spec, .. } = group {
            let has_command = spec.0.iter().any(|c| {
                matches!(c, DesiredCondition::Command { run, .. }
                    if run.contains(&"dbt".to_string()))
            });
            assert!(
                has_command,
                "orders should have a dbt test Command condition"
            );
        } else {
            panic!("expected Conditions");
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
            assert!(spec.on_drift.is_empty());
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
    fn manifest_to_resources_sets_connection_on_all_assets() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin());

        for r in &resources {
            if let NagiKind::Asset { spec, .. } = r {
                assert_eq!(spec.connection.as_deref(), Some("my-bigquery"));
            }
        }
    }

    #[test]
    fn collect_dbt_source_names_deduplicates() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (id_to_name, names) = collect_dbt_source_names(&manifest);

        assert_eq!(id_to_name.len(), 2);
        assert_eq!(names.len(), 2);
        assert_eq!(
            id_to_name.get("source.jaffle_shop.raw.customers").unwrap(),
            "raw.customers"
        );
    }

    #[test]
    fn collect_models_sorted_by_name() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (model_names, model_nodes) = collect_models(&manifest);

        assert_eq!(model_names.len(), 4);
        let names: Vec<&str> = model_nodes.iter().map(|n| n.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["customers", "orders", "stg_customers", "stg_orders"]
        );
    }

    #[test]
    fn collect_tests_groups_by_model() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (model_names, _) = collect_models(&manifest);
        let tests = collect_tests(&manifest, &model_names);

        assert_eq!(tests.get("model.jaffle_shop.customers").unwrap().len(), 2);
        assert_eq!(tests.get("model.jaffle_shop.orders").unwrap().len(), 1);
        assert!(!tests.contains_key("model.jaffle_shop.stg_customers"));
    }

    #[test]
    fn resolve_upstreams_includes_dbt_sources_and_models() {
        let dbt_source_map: HashMap<String, String> = [(
            "source.jaffle_shop.raw.orders".to_string(),
            "raw.orders".to_string(),
        )]
        .into_iter()
        .collect();
        let model_names: HashMap<String, String> = [(
            "model.jaffle_shop.stg_orders".to_string(),
            "stg_orders".to_string(),
        )]
        .into_iter()
        .collect();
        let deps = vec![
            "source.jaffle_shop.raw.orders".to_string(),
            "model.jaffle_shop.stg_orders".to_string(),
            "unknown.id".to_string(),
        ];
        let upstreams = resolve_upstreams(&deps, &dbt_source_map, &model_names);
        assert_eq!(upstreams, vec!["raw.orders", "stg_orders"]);
    }

    #[test]
    fn build_on_drift_returns_empty_without_tests() {
        let (on_drift, conditions) =
            build_on_drift(None, "my_model", &Some("sync-name".to_string()));
        assert!(on_drift.is_empty());
        assert!(conditions.is_none());
    }

    #[test]
    fn build_on_drift_returns_empty_without_default_sync() {
        let test_node = DbtNode {
            unique_id: "test.pkg.t1".to_string(),
            resource_type: "test".to_string(),
            name: "t1".to_string(),
            package_name: "pkg".to_string(),
            tags: vec![],
            depends_on: DbtDependsOn { nodes: vec![] },
            test_metadata: Some(DbtTestMetadata {
                name: "not_null".to_string(),
                kwargs: HashMap::new(),
            }),
        };
        let tests = vec![&test_node];
        let (on_drift, conditions) = build_on_drift(Some(&tests), "my_model", &None);
        assert!(on_drift.is_empty());
        assert!(conditions.is_some(), "Conditions should still be generated");
    }

    #[test]
    fn build_on_drift_with_tests_and_sync() {
        let test_node = DbtNode {
            unique_id: "test.pkg.t1".to_string(),
            resource_type: "test".to_string(),
            name: "t1".to_string(),
            package_name: "pkg".to_string(),
            tags: vec![],
            depends_on: DbtDependsOn { nodes: vec![] },
            test_metadata: Some(DbtTestMetadata {
                name: "not_null".to_string(),
                kwargs: HashMap::new(),
            }),
        };
        let tests = vec![&test_node];
        let (on_drift, conditions) =
            build_on_drift(Some(&tests), "my_model", &Some("default-sync".to_string()));
        assert_eq!(on_drift.len(), 1);
        assert_eq!(on_drift[0].sync, "default-sync");
        assert_eq!(on_drift[0].conditions, "dbt-tests-my_model");
        assert!(conditions.is_some());
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

    // ── build_dbt_source_assets tests ──────────────────────────────────

    #[test]
    fn build_dbt_source_assets_with_tests() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, dbt_source_names) = collect_dbt_source_names(&manifest);
        let dbt_source_tests = collect_tests(&manifest, &dbt_source_map);

        let (resources, needs_skip_sync) = build_dbt_source_assets(
            &dbt_source_names,
            &dbt_source_map,
            &dbt_source_tests,
            "my-bq",
        );

        assert!(needs_skip_sync);

        let assets: Vec<_> = resources.iter().filter(|r| r.kind() == "Asset").collect();
        assert_eq!(assets.len(), 2);

        let conditions: Vec<_> = resources
            .iter()
            .filter(|r| r.kind() == "Conditions")
            .collect();
        assert_eq!(conditions.len(), 1);

        // Sync is not included — caller is responsible for adding it.
        let syncs: Vec<_> = resources.iter().filter(|r| r.kind() == "Sync").collect();
        assert!(syncs.is_empty());
    }

    #[test]
    fn build_dbt_source_assets_without_tests() {
        let names = vec!["raw.customers".to_string()];
        let dbt_source_map = HashMap::new();
        let dbt_source_tests = HashMap::new();

        let (resources, needs_skip_sync) =
            build_dbt_source_assets(&names, &dbt_source_map, &dbt_source_tests, "my-bq");

        assert!(!needs_skip_sync);
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].kind(), "Asset");
        if let NagiKind::Asset { spec, .. } = &resources[0] {
            assert!(spec.on_drift.is_empty());
        }
    }

    // ── build_model_assets tests ───────────────────────────────────────

    #[test]
    fn build_model_assets_includes_upstreams_and_on_drift() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, _) = collect_dbt_source_names(&manifest);
        let (model_names, model_nodes) = collect_models(&manifest);
        let model_tests = collect_tests(&manifest, &model_names);
        let default_sync = Some("dbt-run".to_string());

        let (resources, _) = build_model_assets(
            &model_nodes,
            &model_tests,
            &dbt_source_map,
            &model_names,
            "my-bq",
            &default_sync,
        );

        let assets: Vec<_> = resources.iter().filter(|r| r.kind() == "Asset").collect();
        assert_eq!(assets.len(), 4);

        // customers has model-to-model upstreams
        let customers = assets
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert!(spec.upstreams.contains(&"stg_customers".to_string()));
            assert!(spec.upstreams.contains(&"stg_orders".to_string()));
            assert_eq!(spec.on_drift.len(), 1);
        } else {
            panic!("expected Asset");
        }
    }

    #[test]
    fn build_model_assets_without_default_sync() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, _) = collect_dbt_source_names(&manifest);
        let (model_names, model_nodes) = collect_models(&manifest);
        let model_tests = collect_tests(&manifest, &model_names);

        let (resources, _) = build_model_assets(
            &model_nodes,
            &model_tests,
            &dbt_source_map,
            &model_names,
            "my-bq",
            &None,
        );

        for r in &resources {
            if let NagiKind::Asset { spec, .. } = r {
                assert!(spec.on_drift.is_empty());
            }
        }
    }

    #[test]
    fn build_model_assets_does_not_include_syncs() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, _) = collect_dbt_source_names(&manifest);
        let (model_names, model_nodes) = collect_models(&manifest);
        let model_tests = collect_tests(&manifest, &model_names);

        let (resources, _) = build_model_assets(
            &model_nodes,
            &model_tests,
            &dbt_source_map,
            &model_names,
            "my-bq",
            &Some("dbt-run".to_string()),
        );

        let syncs: Vec<_> = resources.iter().filter(|r| r.kind() == "Sync").collect();
        assert!(
            syncs.is_empty(),
            "Sync generation is the caller's responsibility"
        );
    }

    #[test]
    fn build_model_assets_returns_tag_sets() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, _) = collect_dbt_source_names(&manifest);
        let (model_names, model_nodes) = collect_models(&manifest);
        let model_tests = collect_tests(&manifest, &model_names);

        let (_, tag_sets) = build_model_assets(
            &model_nodes,
            &model_tests,
            &dbt_source_map,
            &model_names,
            "my-bq",
            &Some("dbt-run".to_string()),
        );

        assert!(!tag_sets.is_empty());
        // jaffle_shop has models with tags: ["finance"], ["finance", "daily"]
        assert!(tag_sets.iter().any(|t| t == &["finance"]));
        assert!(tag_sets.iter().any(|t| t == &["daily", "finance"]));
    }

    // ── generate_tag_syncs tests ─────────────────────────────────────────

    #[test]
    fn generate_tag_syncs_from_tag_sets() {
        let tag_sets = vec![
            vec!["finance".to_string()],
            vec!["daily".to_string(), "finance".to_string()],
        ];
        let syncs = generate_tag_syncs(&tag_sets);
        let names: Vec<_> = syncs.iter().map(|s| s.metadata().name.as_str()).collect();
        assert!(names.contains(&"dbt-tag-finance"));
        assert!(names.contains(&"dbt-tag-daily"));
        assert!(names.contains(&"dbt-tag-daily-finance"));
    }

    #[test]
    fn generate_tag_syncs_empty_input() {
        let syncs = generate_tag_syncs(&[]);
        assert!(syncs.is_empty());
    }
}
