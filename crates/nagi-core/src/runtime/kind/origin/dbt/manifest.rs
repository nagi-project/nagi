use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Deserialize;

use crate::runtime::kind::asset::{AssetSpec, DesiredCondition, MergePosition, OnDriftEntry};
use crate::runtime::kind::origin::{DefaultSync, OriginSpec};
use crate::runtime::kind::sync::{SyncSpec, SyncStep};
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
    /// e.g. `model.my_project.orders` or `test.my_project.not_null_orders_id`.
    pub unique_id: String,
    /// `model`, `test`, `seed`, `snapshot`, etc.
    pub resource_type: String,
    /// Node name without package prefix (e.g. `orders`).
    pub name: String,
    #[serde(rename = "package_name")]
    pub _package_name: String,
    /// dbt tags defined on this node.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Nodes this node depends on (upstream references).
    #[serde(default)]
    pub depends_on: DbtDependsOn,
    /// Present when the node is a generic test (not_null, unique, etc.).
    #[serde(default)]
    pub test_metadata: Option<DbtTestMetadata>,
    /// Output database for the model. Used for cross-project resolution.
    #[serde(default)]
    pub database: Option<String>,
    /// Output schema for the model. Used for cross-project resolution.
    #[serde(default)]
    pub schema: Option<String>,
    /// Output table name override. Falls back to `name` when absent.
    #[serde(default)]
    pub alias: Option<String>,
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
    /// Referenced database.
    #[serde(default)]
    pub database: Option<String>,
    /// Referenced schema.
    #[serde(default)]
    pub schema: Option<String>,
    /// Referenced table name override. Falls back to `name` when absent.
    #[serde(default)]
    pub identifier: Option<String>,
}

/// Connection-resolved dbt CLI context passed to `manifest_to_resources`.
#[derive(Default)]
pub struct DbtCliContext<'a> {
    pub profiles_dir: Option<&'a str>,
    pub profile: Option<&'a str>,
    pub target: Option<&'a str>,
}

/// Converts a parsed dbt manifest into Nagi resources.
///
/// Asset names are prefixed with the Origin name: `{origin_name}.{model_name}`.
fn build_dbt_extra_args(project_dir: &str, cli_ctx: &DbtCliContext<'_>) -> Vec<String> {
    let mut args: Vec<String> = vec!["--project-dir".to_string(), project_dir.to_string()];
    if let Some(d) = cli_ctx.profiles_dir {
        args.push("--profiles-dir".to_string());
        args.push(d.to_string());
    }
    if let Some(p) = cli_ctx.profile {
        args.push("--profile".to_string());
        args.push(p.to_string());
    }
    if let Some(t) = cli_ctx.target {
        args.push("--target".to_string());
        args.push(t.to_string());
    }
    args
}

fn resolve_effective_sync(
    default_sync: &Option<DefaultSync>,
    origin_name: &str,
    dbt_extra_args: &[String],
    resources: &mut Vec<NagiKind>,
) -> DefaultSync {
    match default_sync {
        Some(ds) => ds.clone(),
        None => {
            let run_sync_name = format!("{origin_name}-dbt-run");
            resources.push(make_dbt_run_sync(&run_sync_name, dbt_extra_args));
            DefaultSync {
                sync: run_sync_name,
                with: HashMap::new(),
            }
        }
    }
}

pub fn manifest_to_resources(
    manifest: &DbtManifest,
    origin: &OriginSpec,
    origin_name: &str,
    cli_ctx: &DbtCliContext<'_>,
) -> Vec<NagiKind> {
    let OriginSpec::Dbt {
        connection,
        default_sync,
        auto_sync,
        project_dir,
        ..
    } = origin;

    let dbt_extra_args = build_dbt_extra_args(project_dir, cli_ctx);

    let (dbt_source_map, dbt_source_names, dbt_source_tags) =
        collect_dbt_source_names(manifest, origin_name);
    let (model_names, model_nodes) = collect_models(manifest, origin_name);
    let dbt_source_tests = collect_tests(manifest, &dbt_source_map);
    let model_tests = collect_tests(manifest, &model_names);

    let auto_sync_val = auto_sync.unwrap_or(true);
    let ctx = DbtBuildContext {
        connection,
        auto_sync: auto_sync_val,
        dbt_extra_args: &dbt_extra_args,
    };

    let (mut resources, needs_skip_sync) = build_dbt_source_assets(
        &dbt_source_names,
        &dbt_source_map,
        &dbt_source_tests,
        &dbt_source_tags,
        &ctx,
    );
    if needs_skip_sync {
        resources.push(make_skip_sync());
    }

    let effective_sync =
        resolve_effective_sync(default_sync, origin_name, &dbt_extra_args, &mut resources);

    let model_assets = build_dbt_model_assets(
        &model_nodes,
        &model_tests,
        &dbt_source_map,
        &model_names,
        &ctx,
        &effective_sync,
    );
    resources.extend(model_assets);
    resources
}

const SKIP_SYNC_NAME: &str = "nagi-skip-sync";
// ── Auto-generated Sync resources ─────────────────────────────────────

/// Generates the `{origin}-dbt-run` Sync resource: `dbt run --select {{ asset.modelName }}`.
fn make_dbt_run_sync(name: &str, dbt_extra_args: &[String]) -> NagiKind {
    let mut args = vec![
        "dbt".to_string(),
        "run".to_string(),
        "--select".to_string(),
        "{{ asset.modelName }}".to_string(),
    ];
    args.extend_from_slice(dbt_extra_args);
    NagiKind::Sync {
        api_version: API_VERSION.to_string(),
        metadata: Metadata::new(name),
        spec: SyncSpec::new(SyncStep::command(args)),
    }
}

/// Generates the `nagi-skip-sync` Sync resource: runs `true` (exits 0 immediately).
fn make_skip_sync() -> NagiKind {
    NagiKind::Sync {
        api_version: API_VERSION.to_string(),
        metadata: Metadata::new(SKIP_SYNC_NAME),
        spec: SyncSpec::new(SyncStep::command(vec!["true".to_string()])),
    }
}

// ── Asset builders ────────────────────────────────────────────────────

/// Common parameters for building dbt-generated assets.
struct DbtBuildContext<'a> {
    connection: &'a str,
    auto_sync: bool,
    dbt_extra_args: &'a [String],
}

/// Converts dbt tags to Kubernetes-style labels with `dbt/` prefix.
fn dbt_tags_to_labels(tags: &[String]) -> BTreeMap<String, String> {
    tags.iter()
        .map(|tag| (format!("dbt/{tag}"), String::new()))
        .collect()
}

/// Parameters for building a single dbt-generated Asset.
struct DbtAssetParams<'a> {
    name: &'a str,
    labels: BTreeMap<String, String>,
    upstreams: Vec<String>,
    model_name: Option<String>,
    default_sync: &'a DefaultSync,
    tests: Option<&'a Vec<&'a DbtNode>>,
}

/// Builds a single Asset and its optional Conditions resource.
fn build_asset_with_conditions(
    params: &DbtAssetParams<'_>,
    ctx: &DbtBuildContext<'_>,
) -> (NagiKind, Option<NagiKind>) {
    let (on_drift, conditions_resource) = build_on_drift(
        params.tests.filter(|t| !t.is_empty()),
        params.name,
        params.default_sync,
        ctx.dbt_extra_args,
    );

    let asset = NagiKind::Asset {
        api_version: API_VERSION.to_string(),
        metadata: Metadata::with_labels(params.name, params.labels.clone()),
        spec: AssetSpec {
            connection: Some(ctx.connection.to_string()),
            upstreams: params.upstreams.clone(),
            on_drift,
            auto_sync: ctx.auto_sync,
            evaluate_cache_ttl: None,
            model_name: params.model_name.clone(),
        },
    };

    (asset, conditions_resource)
}

/// Re-keys tests from dbt unique_id to nagi Asset name.
fn rekey_tests_by_name<'a>(
    tests: &'a HashMap<String, Vec<&'a DbtNode>>,
    id_to_name: &'a HashMap<String, String>,
) -> HashMap<&'a str, Vec<&'a DbtNode>> {
    let mut by_name: HashMap<&str, Vec<&DbtNode>> = HashMap::new();
    for (uid, test_nodes) in tests {
        if let Some(nagi_name) = id_to_name.get(uid) {
            by_name
                .entry(nagi_name.as_str())
                .or_default()
                .extend(test_nodes.iter().copied());
        }
    }
    by_name
}

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
    let tests_by_name = rekey_tests_by_name(dbt_source_tests, dbt_source_map);
    let skip_sync = DefaultSync {
        sync: SKIP_SYNC_NAME.to_string(),
        with: HashMap::new(),
    };

    for name in names {
        let tests_for_this = tests_by_name.get(name.as_str());
        if tests_for_this.is_some_and(|t| !t.is_empty()) {
            skip_sync_needed = true;
        }

        let params = DbtAssetParams {
            name,
            labels: dbt_tags_to_labels(
                dbt_source_tags
                    .get(name)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]),
            ),
            upstreams: vec![],
            model_name: None,
            default_sync: &skip_sync,
            tests: tests_for_this,
        };
        let (asset, conditions) = build_asset_with_conditions(&params, ctx);
        resources.extend(conditions);
        resources.push(asset);
    }

    (resources, skip_sync_needed)
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
        let prefixed_name = model_names
            .get(&model.unique_id)
            .map(|s| s.as_str())
            .unwrap_or(&model.name);
        let upstreams = resolve_upstreams(&model.depends_on.nodes, dbt_source_map, model_names);

        let params = DbtAssetParams {
            name: prefixed_name,
            labels: dbt_tags_to_labels(&model.tags),
            upstreams,
            model_name: Some(model.name.clone()),
            default_sync,
            tests: model_tests.get(&model.unique_id),
        };
        let (asset, conditions) = build_asset_with_conditions(&params, ctx);
        resources.extend(conditions);
        resources.push(asset);
    }

    resources
}

/// Builds a dbt source unique_id → nagi Asset name lookup and a deduplicated list of names.
/// Returns (id_to_name, sorted_names, name_to_tags).
#[allow(clippy::type_complexity)]
fn collect_dbt_source_names(
    manifest: &DbtManifest,
    origin_name: &str,
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
        let nagi_name = format!("{origin_name}.{}.{}", dbt_src.source_name, dbt_src.name);
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
fn collect_models<'a>(
    manifest: &'a DbtManifest,
    origin_name: &str,
) -> (HashMap<String, String>, Vec<&'a DbtNode>) {
    let mut model_names: HashMap<String, String> = HashMap::new();
    let mut model_nodes: Vec<&DbtNode> = Vec::new();
    for node in manifest.nodes.values() {
        if node.resource_type == "model" {
            model_names.insert(
                node.unique_id.clone(),
                format!("{origin_name}.{}", node.name),
            );
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
    manifest
        .nodes
        .values()
        .filter(|n| n.resource_type == "test")
        .flat_map(|node| {
            node.depends_on
                .nodes
                .iter()
                .filter(|d| valid_ids.contains_key(*d))
                .map(move |dep| (dep.clone(), node))
        })
        .fold(HashMap::new(), |mut acc, (dep, node)| {
            acc.entry(dep).or_insert_with(Vec::new).push(node);
            acc
        })
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
        metadata: Metadata::new(group_name.clone()),
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
                identity: None,
            });
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORIGIN_NAME: &str = "jaffle";

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
            env: HashMap::new(),
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
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let assets: Vec<_> = resources.iter().filter(|r| r.kind() == "Asset").collect();
        // 2 dbt source Assets + 4 model Assets
        assert_eq!(assets.len(), 6);

        let asset_names: HashSet<_> = assets.iter().map(|a| a.metadata().name.as_str()).collect();
        assert!(asset_names.contains("jaffle.raw.customers"));
        assert!(asset_names.contains("jaffle.raw.orders"));
        assert!(asset_names.contains("jaffle.customers"));
        assert!(asset_names.contains("jaffle.orders"));
        assert!(asset_names.contains("jaffle.stg_customers"));
        assert!(asset_names.contains("jaffle.stg_orders"));
    }

    #[test]
    fn manifest_to_resources_sets_model_name_on_model_assets() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let orders = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.orders")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = orders {
            assert_eq!(
                spec.model_name.as_deref(),
                Some("orders"),
                "model Asset should have model_name set to the original dbt model name"
            );
        } else {
            panic!("expected Asset");
        }
    }

    #[test]
    fn manifest_to_resources_source_assets_have_no_model_name() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let raw_customers = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.raw.customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = raw_customers {
            assert!(
                spec.model_name.is_none(),
                "source Assets should not have model_name set"
            );
        } else {
            panic!("expected Asset");
        }
    }

    #[test]
    fn manifest_to_resources_maps_tags() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let orders = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.orders")
            .unwrap();
        if let NagiKind::Asset { metadata, .. } = orders {
            assert!(metadata.labels.contains_key("dbt/finance"));
            assert!(metadata.labels.contains_key("dbt/daily"));
        } else {
            panic!("orders should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_maps_dbt_source_tags() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let raw_orders = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.raw.orders")
            .unwrap();
        if let NagiKind::Asset { metadata, .. } = raw_orders {
            assert!(metadata.labels.contains_key("dbt/external"));
        } else {
            panic!("raw.orders should be an Asset");
        }

        let raw_customers = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.raw.customers")
            .unwrap();
        if let NagiKind::Asset { metadata, .. } = raw_customers {
            assert!(metadata.labels.is_empty());
        } else {
            panic!("raw.customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_resolves_upstreams() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        // stg_customers depends on raw.customers
        let stg_customers = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.stg_customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = stg_customers {
            assert_eq!(spec.upstreams, vec!["jaffle.raw.customers"]);
        } else {
            panic!("stg_customers should be an Asset");
        }

        // customers depends on model stg_customers and stg_orders
        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert!(spec.upstreams.contains(&"jaffle.stg_customers".to_string()));
            assert!(spec.upstreams.contains(&"jaffle.stg_orders".to_string()));
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_applies_default_sync_via_on_drift() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert_eq!(spec.on_drift.len(), 1);
            assert_eq!(spec.on_drift[0].sync, "dbt-default");
            assert_eq!(spec.on_drift[0].conditions, "dbt-tests-jaffle.customers");
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_applies_dbt_source_tests() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        // raw.orders has a not_null test → should have on_drift with nagi-skip-sync
        let raw_orders = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.raw.orders")
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
            .find(|r| r.metadata().name == "jaffle.raw.customers")
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
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let group = resources
            .iter()
            .find(|r| r.metadata().name == "dbt-tests-jaffle.customers")
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
    fn manifest_to_resources_no_default_sync_generates_dbt_run() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let origin = OriginSpec::Dbt {
            connection: "my-bq".to_string(),
            project_dir: "../dbt-project".to_string(),
            default_sync: None,
            auto_sync: None,
            env: HashMap::new(),
        };
        let resources =
            manifest_to_resources(&manifest, &origin, ORIGIN_NAME, &DbtCliContext::default());

        // Auto-generated {origin}-dbt-run Sync should be present
        let dbt_run_sync = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle-dbt-run");
        assert!(
            dbt_run_sync.is_some(),
            "jaffle-dbt-run Sync should be auto-generated"
        );
        if let Some(NagiKind::Sync { spec, .. }) = dbt_run_sync {
            assert_eq!(
                spec.run.args,
                &[
                    "dbt",
                    "run",
                    "--select",
                    "{{ asset.modelName }}",
                    "--project-dir",
                    "../dbt-project",
                ]
            );
        }

        // Model Assets with tests should reference {origin}-dbt-run
        let customers = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle.customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert_eq!(spec.on_drift.len(), 1);
            assert_eq!(spec.on_drift[0].sync, "jaffle-dbt-run");
        } else {
            panic!("customers should be an Asset");
        }
    }

    #[test]
    fn manifest_to_resources_sync_includes_profile_and_target() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let origin = OriginSpec::Dbt {
            connection: "my-bq".to_string(),
            project_dir: "../dbt-project".to_string(),
            default_sync: None,
            auto_sync: None,
            env: HashMap::new(),
        };
        let cli_ctx = DbtCliContext {
            profiles_dir: Some("/path/to/profiles"),
            profile: Some("my_project"),
            target: Some("prod"),
        };
        let resources = manifest_to_resources(&manifest, &origin, ORIGIN_NAME, &cli_ctx);

        let dbt_run_sync = resources
            .iter()
            .find(|r| r.metadata().name == "jaffle-dbt-run")
            .unwrap();
        if let NagiKind::Sync { spec, .. } = dbt_run_sync {
            assert_eq!(
                spec.run.args,
                &[
                    "dbt",
                    "run",
                    "--select",
                    "{{ asset.modelName }}",
                    "--project-dir",
                    "../dbt-project",
                    "--profiles-dir",
                    "/path/to/profiles",
                    "--profile",
                    "my_project",
                    "--target",
                    "prod",
                ]
            );
        } else {
            panic!("expected Sync");
        }
    }

    #[test]
    fn manifest_to_resources_default_sync_suppresses_auto_generation() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let dbt_run_sync = resources
            .iter()
            .any(|r| r.metadata().name == "jaffle-dbt-run");
        assert!(
            !dbt_run_sync,
            "jaffle-dbt-run should not be generated when defaultSync is set"
        );
    }

    #[test]
    fn manifest_to_resources_ignores_seeds() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        let has_seed = resources.iter().any(|r| {
            r.kind() == "Asset"
                && (r.metadata().name == "raw_customers"
                    || r.metadata().name == "jaffle.raw_customers")
        });
        assert!(!has_seed, "seeds should not be converted to Assets");
    }

    #[test]
    fn manifest_to_resources_sets_connection_on_all_assets() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let resources = manifest_to_resources(
            &manifest,
            &jaffle_shop_origin(),
            ORIGIN_NAME,
            &DbtCliContext::default(),
        );

        for r in &resources {
            if let NagiKind::Asset { spec, .. } = r {
                assert_eq!(spec.connection.as_deref(), Some("my-bigquery"));
            }
        }
    }

    #[test]
    fn collect_dbt_source_names_deduplicates() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (id_to_name, names, _dbt_source_tags) =
            collect_dbt_source_names(&manifest, ORIGIN_NAME);

        assert_eq!(id_to_name.len(), 2);
        assert_eq!(names.len(), 2);
        assert_eq!(
            id_to_name.get("source.jaffle_shop.raw.customers").unwrap(),
            "jaffle.raw.customers"
        );
    }

    #[test]
    fn collect_models_sorted_by_name() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (model_names, model_nodes) = collect_models(&manifest, ORIGIN_NAME);

        assert_eq!(model_names.len(), 4);
        // model_nodes are sorted by raw name; Origin-prefixed names are in model_names map
        let names: Vec<&str> = model_nodes.iter().map(|n| n.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["customers", "orders", "stg_customers", "stg_orders"]
        );
        // model_names values are Origin-prefixed
        assert_eq!(
            model_names.get("model.jaffle_shop.customers").unwrap(),
            "jaffle.customers"
        );
    }

    #[test]
    fn collect_tests_groups_by_model() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (model_names, _) = collect_models(&manifest, ORIGIN_NAME);
        let tests = collect_tests(&manifest, &model_names);

        assert_eq!(tests.get("model.jaffle_shop.customers").unwrap().len(), 2);
        assert_eq!(tests.get("model.jaffle_shop.orders").unwrap().len(), 1);
        assert!(!tests.contains_key("model.jaffle_shop.stg_customers"));
    }

    #[test]
    fn resolve_upstreams_includes_dbt_sources_and_models() {
        let dbt_source_map: HashMap<String, String> = [(
            "source.jaffle_shop.raw.orders".to_string(),
            "jaffle.raw.orders".to_string(),
        )]
        .into_iter()
        .collect();
        let model_names: HashMap<String, String> = [(
            "model.jaffle_shop.stg_orders".to_string(),
            "jaffle.stg_orders".to_string(),
        )]
        .into_iter()
        .collect();
        let deps = vec![
            "source.jaffle_shop.raw.orders".to_string(),
            "model.jaffle_shop.stg_orders".to_string(),
            "unknown.id".to_string(),
        ];
        let upstreams = resolve_upstreams(&deps, &dbt_source_map, &model_names);
        assert_eq!(upstreams, vec!["jaffle.raw.orders", "jaffle.stg_orders"]);
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
            database: None,
            schema: None,
            alias: None,
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
            collect_dbt_source_names(&manifest, ORIGIN_NAME);
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
        let names = vec!["jaffle.raw.customers".to_string()];
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
        let (dbt_source_map, _, _) = collect_dbt_source_names(&manifest, ORIGIN_NAME);
        let (model_names, model_nodes) = collect_models(&manifest, ORIGIN_NAME);
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
            .find(|r| r.metadata().name == "jaffle.customers")
            .unwrap();
        if let NagiKind::Asset { spec, .. } = customers {
            assert!(spec.upstreams.contains(&"jaffle.stg_customers".to_string()));
            assert!(spec.upstreams.contains(&"jaffle.stg_orders".to_string()));
            assert_eq!(spec.on_drift.len(), 1);
        } else {
            panic!("expected Asset");
        }
    }

    #[test]
    fn build_dbt_model_assets_with_custom_sync_with() {
        let manifest: DbtManifest = serde_json::from_str(jaffle_shop_manifest_json()).unwrap();
        let (dbt_source_map, _, _) = collect_dbt_source_names(&manifest, ORIGIN_NAME);
        let (model_names, model_nodes) = collect_models(&manifest, ORIGIN_NAME);
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
            .find(|r| r.metadata().name == "jaffle.customers")
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
        let (dbt_source_map, _, _) = collect_dbt_source_names(&manifest, ORIGIN_NAME);
        let (model_names, model_nodes) = collect_models(&manifest, ORIGIN_NAME);
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
            env: HashMap::new(),
        };
        let resources =
            manifest_to_resources(&manifest, &origin, ORIGIN_NAME, &DbtCliContext::default());

        for r in &resources {
            if let NagiKind::Asset { spec, .. } = r {
                assert!(
                    !spec.auto_sync,
                    "all generated Assets should have autoSync: false"
                );
            }
        }
    }

    #[test]
    fn build_dbt_extra_args_project_dir_only() {
        let ctx = DbtCliContext::default();
        let args = build_dbt_extra_args("/project", &ctx);
        assert_eq!(args, vec!["--project-dir", "/project"]);
    }

    #[test]
    fn build_dbt_extra_args_all_options() {
        let ctx = DbtCliContext {
            profiles_dir: Some("/profiles"),
            profile: Some("my_profile"),
            target: Some("prod"),
        };
        let args = build_dbt_extra_args("/project", &ctx);
        assert_eq!(
            args,
            vec![
                "--project-dir",
                "/project",
                "--profiles-dir",
                "/profiles",
                "--profile",
                "my_profile",
                "--target",
                "prod"
            ]
        );
    }

    #[test]
    fn rekey_tests_by_name_maps_uid_to_nagi_name() {
        let test_node = DbtNode {
            unique_id: "test.t1".to_string(),
            name: "t1".to_string(),
            resource_type: "test".to_string(),
            depends_on: DbtDependsOn { nodes: vec![] },
            tags: vec![],
            test_metadata: None,
            _package_name: String::new(),
            database: None,
            schema: None,
            alias: None,
        };
        let tests: HashMap<String, Vec<&DbtNode>> =
            HashMap::from([("model.m1".to_string(), vec![&test_node])]);
        let id_to_name: HashMap<String, String> =
            HashMap::from([("model.m1".to_string(), "origin.m1".to_string())]);

        let result = rekey_tests_by_name(&tests, &id_to_name);
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("origin.m1"));
        assert_eq!(result["origin.m1"].len(), 1);
    }
}
