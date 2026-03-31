use std::collections::{HashMap, HashSet};

use serde::Deserialize;

use crate::runtime::kind::asset::{AssetSpec, DesiredCondition, MergePosition, OnDriftEntry};
use crate::runtime::kind::origin::{DefaultSync, OriginSpec};
use crate::runtime::kind::sync::{StepType, SyncSpec, SyncStep};
use crate::runtime::kind::{Metadata, NagiKind, API_VERSION};

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
    #[serde(rename = "package_name")]
    pub _package_name: String,
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
    #[serde(rename = "name")]
    pub _name: String,
    #[serde(default, rename = "kwargs")]
    pub _kwargs: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct DbtSource {
    pub unique_id: String,
    pub name: String,
    pub source_name: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Converts a parsed dbt manifest into Nagi resources.
pub fn manifest_to_resources(
    manifest: &DbtManifest,
    origin: &OriginSpec,
    profiles_dir: Option<&str>,
) -> Vec<NagiKind> {
    let OriginSpec::Dbt {
        connection,
        default_sync,
        auto_sync,
        project_dir,
        ..
    } = origin;

    // Build extra args for dbt commands (--project-dir, --profiles-dir).
    let mut dbt_extra_args: Vec<String> = vec!["--project-dir".to_string(), project_dir.clone()];
    if let Some(d) = profiles_dir {
        dbt_extra_args.push("--profiles-dir".to_string());
        dbt_extra_args.push(d.to_string());
    }

    let (dbt_source_map, dbt_source_names, dbt_source_tags) = collect_dbt_source_names(manifest);
    let (model_names, model_nodes) = collect_models(manifest);
    let dbt_source_tests = collect_tests(manifest, &dbt_source_map);
    let model_tests = collect_tests(manifest, &model_names);

    let auto_sync_val = auto_sync.unwrap_or(true);
    let source_ctx = DbtBuildContext {
        connection,
        auto_sync: auto_sync_val,
        dbt_extra_args: &dbt_extra_args,
    };
    let (mut resources, needs_skip_sync) = build_dbt_source_assets(
        &dbt_source_names,
        &dbt_source_map,
        &dbt_source_tests,
        &dbt_source_tags,
        &source_ctx,
    );
    if needs_skip_sync {
        resources.push(make_skip_sync());
    }

    // When defaultSync is not set, auto-generate the nagi-dbt-run Sync.
    let effective_sync = match default_sync {
        Some(ds) => ds.clone(),
        None => {
            resources.push(make_dbt_run_sync_with_args(&dbt_extra_args));
            DefaultSync {
                sync: DBT_RUN_SYNC_NAME.to_string(),
                with: HashMap::new(),
            }
        }
    };

    let model_ctx = DbtBuildContext {
        connection,
        auto_sync: auto_sync_val,
        dbt_extra_args: &dbt_extra_args,
    };
    let model_assets = build_dbt_model_assets(
        &model_nodes,
        &model_tests,
        &dbt_source_map,
        &model_names,
        &model_ctx,
        &effective_sync,
    );
    resources.extend(model_assets);
    resources
}

const SKIP_SYNC_NAME: &str = "nagi-skip-sync";
const DBT_RUN_SYNC_NAME: &str = "nagi-dbt-run";

// ── Auto-generated Sync resources ─────────────────────────────────────

/// Generates the `nagi-dbt-run` Sync resource: `dbt run --select {{ asset.name }}`.
fn make_dbt_run_sync_with_args(dbt_extra_args: &[String]) -> NagiKind {
    let mut args = vec![
        "dbt".to_string(),
        "run".to_string(),
        "--select".to_string(),
        "{{ asset.name }}".to_string(),
    ];
    args.extend_from_slice(dbt_extra_args);
    NagiKind::Sync {
        api_version: API_VERSION.to_string(),
        metadata: Metadata {
            name: DBT_RUN_SYNC_NAME.to_string(),
        },
        spec: SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args,
                env: HashMap::new(),
            },
            post: None,
        },
    }
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

// ── Asset builders ────────────────────────────────────────────────────

/// Builds Assets and Conditions for dbt sources (external tables).
/// Returns the resources and whether `nagi-skip-sync` is needed.
fn build_dbt_source_assets(
    names: &[String],
    dbt_source_map: &HashMap<String, String>,
    dbt_source_tests: &HashMap<String, Vec<&DbtNode>>,
    dbt_source_tags: &HashMap<String, Vec<String>>,
    ctx: &DbtBuildContext<'_>,
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

        if has_tests {
            skip_sync_needed = true;
        }
        let skip_sync = DefaultSync {
            sync: SKIP_SYNC_NAME.to_string(),
            with: HashMap::new(),
        };
        let (on_drift, conditions_resource) = build_on_drift(
            tests_for_this.filter(|t| !t.is_empty()),
            name,
            &skip_sync,
            ctx.dbt_extra_args,
        );

        if let Some(cond) = conditions_resource {
            resources.push(cond);
        }

        resources.push(NagiKind::Asset {
            api_version: API_VERSION.to_string(),
            metadata: Metadata { name: name.clone() },
            spec: AssetSpec {
                tags: dbt_source_tags.get(name).cloned().unwrap_or_default(),
                connection: Some(ctx.connection.to_string()),
                upstreams: vec![],
                on_drift,
                auto_sync: ctx.auto_sync,
                evaluate_cache_ttl: None,
            },
        });
    }

    (resources, skip_sync_needed)
}

/// Common parameters for building dbt-generated assets.
struct DbtBuildContext<'a> {
    connection: &'a str,
    auto_sync: bool,
    dbt_extra_args: &'a [String],
}

/// Builds Assets and Conditions for dbt models.
fn build_dbt_model_assets(
    model_nodes: &[&DbtNode],
    model_tests: &HashMap<String, Vec<&DbtNode>>,
    dbt_source_map: &HashMap<String, String>,
    model_names: &HashMap<String, String>,
    ctx: &DbtBuildContext<'_>,
    default_sync: &DefaultSync,
) -> Vec<NagiKind> {
    let mut resources: Vec<NagiKind> = Vec::new();

    for model in model_nodes {
        let upstreams = resolve_upstreams(&model.depends_on.nodes, dbt_source_map, model_names);

        let (on_drift, conditions_resource) = build_on_drift(
            model_tests.get(&model.unique_id),
            &model.name,
            default_sync,
            ctx.dbt_extra_args,
        );

        if let Some(cond) = conditions_resource {
            resources.push(cond);
        }

        resources.push(NagiKind::Asset {
            api_version: API_VERSION.to_string(),
            metadata: Metadata {
                name: model.name.clone(),
            },
            spec: AssetSpec {
                tags: model.tags.clone(),
                connection: Some(ctx.connection.to_string()),
                upstreams,
                on_drift,
                auto_sync: ctx.auto_sync,
                evaluate_cache_ttl: None,
            },
        });
    }

    resources
}

/// Builds a dbt source unique_id → nagi Asset name lookup and a deduplicated list of names.
/// Returns (id_to_name, sorted_names, name_to_tags).
#[allow(clippy::type_complexity)]
fn collect_dbt_source_names(
    manifest: &DbtManifest,
) -> (
    HashMap<String, String>,
    Vec<String>,
    HashMap<String, Vec<String>>,
) {
    let mut id_to_name: HashMap<String, String> = HashMap::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut names: Vec<String> = Vec::new();
    let mut name_to_tags: HashMap<String, Vec<String>> = HashMap::new();

    for dbt_src in manifest.sources.values() {
        let nagi_name = format!("{}.{}", dbt_src.source_name, dbt_src.name);
        id_to_name.insert(dbt_src.unique_id.clone(), nagi_name.clone());
        name_to_tags.insert(nagi_name.clone(), dbt_src.tags.clone());
        if seen.insert(nagi_name.clone()) {
            names.push(nagi_name);
        }
    }
    names.sort();
    (id_to_name, names, name_to_tags)
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
    default_sync: &DefaultSync,
    dbt_extra_args: &[String],
) -> (Vec<OnDriftEntry>, Option<NagiKind>) {
    let conditions = tests
        .map(|t| tests_to_conditions(t, dbt_extra_args))
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
        spec: crate::runtime::kind::condition::ConditionsSpec(conditions),
    };

    let on_drift = vec![OnDriftEntry {
        conditions: group_name,
        sync: default_sync.sync.clone(),
        with: default_sync.with.clone(),
        merge_position: MergePosition::BeforeOrigin,
    }];

    (on_drift, Some(conditions_resource))
}

/// Converts dbt tests into Nagi DesiredCondition entries.
/// All tests are executed via `dbt test --select` to ensure behavior matches
/// dbt's own test implementation exactly.
fn tests_to_conditions(tests: &[&DbtNode], dbt_extra_args: &[String]) -> Vec<DesiredCondition> {
    let mut entries = Vec::new();
    for test in tests {
        if test.test_metadata.is_some() {
            let mut run = vec![
                "dbt".to_string(),
                "test".to_string(),
                "--select".to_string(),
                test.name.clone(),
            ];
            run.extend_from_slice(dbt_extra_args);
            entries.push(DesiredCondition::Command {
                name: format!("dbt-test-{}", test.name),
                run,
                interval: None,
                env: HashMap::new(),
                evaluate_cache_ttl: None,
            });
        }
    }
    entries
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
      "source_name": "raw",
      "tags": ["external"]
    }
  }
}"#
    }

    fn jaffle_shop_origin() -> OriginSpec {
        OriginSpec::Dbt {
            connection: "my-bigquery".to_string(),
            project_dir: "../dbt-project".to_string(),
            default_sync: Some(DefaultSync {
                sync: "dbt-default".to_string(),
                with: HashMap::new(),
            }),
            auto_sync: None,
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
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

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
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

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
    fn manifest_to_resources_maps_dbt_source_tags() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

        let raw_orders = resources
            .iter()
            .find(|r| r.metadata().name == "raw.orders")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = raw_orders {
            assert_eq!(spec.tags, vec!["external"]);
        } else {
            panic!("raw.orders should be an Asset");
        }

        let raw_customers = resources
            .iter()
            .find(|r| r.metadata().name == "raw.customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = raw_customers {
            assert!(spec.tags.is_empty());
        } else {
            panic!("raw.customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_resolves_upstreams() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

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
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

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
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

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
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

        let group = resources
            .iter()
            .find(|r| r.metadata().name == "dbt-tests-customers")
            .unwrap();
        if let NagiKind::Conditions { spec, .. } = group {
            let has_not_null = spec.0.iter().any(|c| {
                matches!(c, DesiredCondition::Command { name, run, .. }
                    if name == "dbt-test-not_null_customers_customer_id"
                    && run.len() >= 4
                    && run[..4] == ["dbt", "test", "--select", "not_null_customers_customer_id"])
            });
            assert!(has_not_null, "should have a not_null Command condition");

            let has_unique = spec.0.iter().any(|c| {
                matches!(c, DesiredCondition::Command { name, run, .. }
                    if name == "dbt-test-unique_customers_customer_id"
                    && run.len() >= 4
                    && run[..4] == ["dbt", "test", "--select", "unique_customers_customer_id"])
            });
            assert!(has_unique, "should have a unique Command condition");
        } else {
            panic!("expected Conditions");
        }
    }

    #[test]
    fn manifest_to_resources_maps_generic_test_as_command() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

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
    fn manifest_to_resources_no_default_sync_generates_dbt_run() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let origin = OriginSpec::Dbt {
            connection: "my-bq".to_string(),
            project_dir: "../dbt-project".to_string(),
            default_sync: None,
            auto_sync: None,
        };
        let resources = manifest_to_resources(&manifest, &origin, None);

        // Auto-generated nagi-dbt-run Sync should be present
        let dbt_run_sync = resources
            .iter()
            .find(|r| r.metadata().name == "nagi-dbt-run");
        assert!(
            dbt_run_sync.is_some(),
            "nagi-dbt-run Sync should be auto-generated"
        );
        if let Some(NagiKind::Sync { spec, .. }) = dbt_run_sync {
            assert!(spec.run.args.len() >= 4);
            assert_eq!(
                &spec.run.args[..4],
                &["dbt", "run", "--select", "{{ asset.name }}"]
            );
        }

        // Model Assets with tests should reference nagi-dbt-run
        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert_eq!(spec.on_drift.len(), 1);
            assert_eq!(spec.on_drift[0].sync, "nagi-dbt-run");
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_default_sync_suppresses_auto_generation() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

        let dbt_run_sync = resources
            .iter()
            .any(|r| r.metadata().name == "nagi-dbt-run");
        assert!(
            !dbt_run_sync,
            "nagi-dbt-run should not be generated when defaultSync is set"
        );
    }

    #[test]
    fn manifest_to_resources_ignores_seeds() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

        let has_seed = resources
            .iter()
            .any(|r| r.metadata().name == "raw_customers" && r.kind() == "Asset");
        assert!(!has_seed, "seeds should not be converted to Assets");
    }

    #[test]
    fn manifest_to_resources_sets_connection_on_all_assets() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(&manifest, &jaffle_shop_origin(), None);

        for r in &resources {
            if let NagiKind::Asset { spec, .. } = r {
                assert_eq!(spec.connection.as_deref(), Some("my-bigquery"));
            }
        }
    }

    #[test]
    fn collect_dbt_source_names_deduplicates() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (id_to_name, names, _dbt_source_tags) = collect_dbt_source_names(&manifest);

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
        let ds = DefaultSync {
            sync: "nagi-dbt-run".to_string(),
            with: HashMap::new(),
        };
        let (on_drift, conditions) = build_on_drift(None, "my_model", &ds, &[]);
        assert!(on_drift.is_empty());
        assert!(conditions.is_none());
    }

    #[test]
    fn build_on_drift_with_tests_and_sync() {
        let test_node = DbtNode {
            unique_id: "test.pkg.t1".to_string(),
            resource_type: "test".to_string(),
            name: "t1".to_string(),
            _package_name: "pkg".to_string(),
            tags: vec![],
            depends_on: DbtDependsOn { nodes: vec![] },
            test_metadata: Some(DbtTestMetadata {
                _name: "not_null".to_string(),
                _kwargs: HashMap::new(),
            }),
        };
        let tests = vec![&test_node];
        let ds = DefaultSync {
            sync: "default-sync".to_string(),
            with: HashMap::new(),
        };
        let (on_drift, conditions) = build_on_drift(Some(&tests), "my_model", &ds, &[]);
        assert_eq!(on_drift.len(), 1);
        assert_eq!(on_drift[0].sync, "default-sync");
        assert_eq!(on_drift[0].conditions, "dbt-tests-my_model");
        assert!(conditions.is_some());
    }

    // ── build_dbt_source_assets tests ──────────────────────────────────

    #[test]
    fn build_dbt_source_assets_with_tests() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, dbt_source_names, dbt_source_tags) =
            collect_dbt_source_names(&manifest);
        let dbt_source_tests = collect_tests(&manifest, &dbt_source_map);

        let ctx = DbtBuildContext {
            connection: "my-bq",
            auto_sync: true,
            dbt_extra_args: &[],
        };
        let (resources, needs_skip_sync) = build_dbt_source_assets(
            &dbt_source_names,
            &dbt_source_map,
            &dbt_source_tests,
            &dbt_source_tags,
            &ctx,
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
        let dbt_source_tags = HashMap::new();

        let ctx = DbtBuildContext {
            connection: "my-bq",
            auto_sync: true,
            dbt_extra_args: &[],
        };
        let (resources, needs_skip_sync) = build_dbt_source_assets(
            &names,
            &dbt_source_map,
            &dbt_source_tests,
            &dbt_source_tags,
            &ctx,
        );

        assert!(!needs_skip_sync);
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].kind(), "Asset");
        if let NagiKind::Asset { spec, .. } = &resources[0] {
            assert!(spec.on_drift.is_empty());
        }
    }

    // ── build_dbt_model_assets tests ───────────────────────────────────────

    #[test]
    fn build_dbt_model_assets_includes_upstreams_and_on_drift() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, _, _) = collect_dbt_source_names(&manifest);
        let (model_names, model_nodes) = collect_models(&manifest);
        let model_tests = collect_tests(&manifest, &model_names);
        let ds = DefaultSync {
            sync: "dbt-run".to_string(),
            with: HashMap::new(),
        };
        let ctx = DbtBuildContext {
            connection: "my-bq",
            auto_sync: true,
            dbt_extra_args: &[],
        };
        let resources = build_dbt_model_assets(
            &model_nodes,
            &model_tests,
            &dbt_source_map,
            &model_names,
            &ctx,
            &ds,
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
    fn build_dbt_model_assets_with_custom_sync_with() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, _, _) = collect_dbt_source_names(&manifest);
        let (model_names, model_nodes) = collect_models(&manifest);
        let model_tests = collect_tests(&manifest, &model_names);
        let ds = DefaultSync {
            sync: "custom-sync".to_string(),
            with: HashMap::from([("selector".to_string(), "+{{ asset.name }}".to_string())]),
        };

        let ctx = DbtBuildContext {
            connection: "my-bq",
            auto_sync: true,
            dbt_extra_args: &[],
        };
        let resources = build_dbt_model_assets(
            &model_nodes,
            &model_tests,
            &dbt_source_map,
            &model_names,
            &ctx,
            &ds,
        );

        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert_eq!(spec.on_drift[0].sync, "custom-sync");
            assert_eq!(
                spec.on_drift[0].with.get("selector").unwrap(),
                "+{{ asset.name }}"
            );
        } else {
            panic!("expected Asset");
        }
    }

    #[test]
    fn build_dbt_model_assets_does_not_include_syncs() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, _, _) = collect_dbt_source_names(&manifest);
        let (model_names, model_nodes) = collect_models(&manifest);
        let model_tests = collect_tests(&manifest, &model_names);

        let ds = DefaultSync {
            sync: "dbt-run".to_string(),
            with: HashMap::new(),
        };
        let ctx = DbtBuildContext {
            connection: "my-bq",
            auto_sync: true,
            dbt_extra_args: &[],
        };
        let resources = build_dbt_model_assets(
            &model_nodes,
            &model_tests,
            &dbt_source_map,
            &model_names,
            &ctx,
            &ds,
        );

        let syncs: Vec<_> = resources.iter().filter(|r| r.kind() == "Sync").collect();
        assert!(
            syncs.is_empty(),
            "Sync generation is the caller's responsibility"
        );
    }

    #[test]
    fn origin_auto_sync_false_propagates_to_assets() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let origin = OriginSpec::Dbt {
            connection: "my-bq".to_string(),
            project_dir: "../dbt-project".to_string(),
            default_sync: Some(DefaultSync {
                sync: "dbt-default".to_string(),
                with: HashMap::new(),
            }),
            auto_sync: Some(false),
        };
        let resources = manifest_to_resources(&manifest, &origin, None);

        for r in &resources {
            if let NagiKind::Asset { spec, .. } = r {
                assert!(
                    !spec.auto_sync,
                    "all generated Assets should have autoSync: false"
                );
            }
        }
    }
}
