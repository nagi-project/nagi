mod categorize;
pub mod dbt;
mod load;

pub(crate) use load::load_compiled_assets;
pub use load::load_resources;
pub(crate) use load::{load_graph, resolve_compiled_asset_names};

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::runtime::kind::asset::{
    self as asset, validate_no_duplicate_condition_names, AssetSpec, DesiredCondition,
};
use crate::runtime::kind::sync::{SyncSpec, SyncStep};
use crate::runtime::kind::{self, KindError, Metadata, NagiKind};

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("failed to read assets directory: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Kind(#[from] KindError),

    #[error("unresolved reference: {kind} '{name}' not found")]
    UnresolvedRef { kind: String, name: String },

    #[error("duplicate resource: {kind} '{name}' defined more than once")]
    DuplicateName { kind: String, name: String },

    #[error("dependency cycle detected involving '{name}'")]
    CycleDetected { name: String },

    #[error("origin expansion failed: {0}")]
    OriginFailed(String),

    #[error("manifest.json parse error: {0}")]
    ManifestParse(String),

    #[error("invalid kind filter: '{0}'. Valid values: Asset, Connection, Conditions, Sync")]
    InvalidKind(String),

    #[error("dbt Cloud API error: {0}")]
    DbtCloud(String),

    #[error("failed to create async runtime: {0}")]
    Runtime(String),

    #[error("{}", format_multiple_errors(.0))]
    Multiple(Vec<CompileError>),
}

fn format_multiple_errors(errors: &[CompileError]) -> String {
    errors
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Converts a vec of errors into a single Result.
/// Empty vec → Ok, single error → that error, multiple → Multiple.
fn into_result(errors: Vec<CompileError>) -> Result<(), CompileError> {
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.into_iter().next().unwrap()),
        _ => Err(CompileError::Multiple(errors)),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DependencyGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphNode {
    pub name: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
}

#[derive(Debug)]
pub struct CompileOutput {
    pub assets: Vec<ResolvedAsset>,
    pub graph: DependencyGraph,
}

#[derive(Debug, Clone)]
pub struct ResolvedAsset {
    pub metadata: Metadata,
    /// Original model name without the Origin prefix.
    /// For Origin-generated Assets this is the dbt model name (e.g. "orders").
    /// For user-defined Assets this equals `metadata.name`.
    pub model_name: String,
    pub spec: AssetSpec,
    /// Resolved on_drift entries: conditions expanded + sync specs resolved.
    pub resolved_on_drift: Vec<ResolvedOnDriftEntry>,
    pub connection: Option<ResolvedConnection>,
    /// dbt Cloud job IDs whose execute_steps include this asset.
    pub dbt_cloud_job_ids: Option<HashSet<i64>>,
}

/// A compiled on_drift entry with resolved conditions and sync spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedOnDriftEntry {
    /// Resolved conditions from the referenced Conditions.
    pub conditions: Vec<DesiredCondition>,
    /// Name of the conditions group (for display/logging).
    pub conditions_ref: String,
    /// Resolved and template-expanded sync spec.
    pub sync: SyncSpec,
    /// Name of the sync ref (for lock coordination).
    pub sync_ref_name: String,
}

/// Compiles all YAML resources from `resources_dir` and writes resolved output to `target_dir`.
/// When `export_config` is provided, auto-generates export Assets for log tables.
pub fn compile(
    resources_dir: &Path,
    target_dir: &Path,
    export_config: Option<&crate::runtime::config::ExportConfig>,
) -> Result<CompileOutput, CompileError> {
    let resources = load_resources(resources_dir)?;

    let mut resources = crate::runtime::kind::origin::generate(resources)?;

    if let Some(cfg) = export_config {
        resources.extend(crate::runtime::export::generate_export_resources(cfg));
    }

    let mut output = resolve(resources)?;

    if let Some(cred_path) = dbt::find_dbt_cloud_credentials(&output) {
        let rt =
            tokio::runtime::Runtime::new().map_err(|e| CompileError::Runtime(e.to_string()))?;
        let mapping = rt
            .block_on(
                crate::runtime::kind::origin::dbt::cloud::fetch_job_model_mapping(
                    std::path::Path::new(&cred_path),
                ),
            )
            .map_err(|e| CompileError::DbtCloud(e.to_string()))?;
        dbt::apply_cloud_job_mapping(&mut output, &mapping);
    }

    write_output(&output, target_dir)?;
    Ok(output)
}

use crate::runtime::kind::connection::{
    connection_identity_ref, resolve_connection_by_name, ConnectionSpec, ResolvedConnection,
};

use categorize::{categorize, CategorizedResources};

fn require_sync_ref(syncs: &HashMap<String, SyncSpec>, name: &str) -> Result<(), CompileError> {
    if !syncs.contains_key(name) {
        return Err(CompileError::UnresolvedRef {
            kind: "Sync".to_string(),
            name: name.to_string(),
        });
    }
    Ok(())
}

/// Expands template variables in a SyncSpec's args.
///
/// Supported variables:
/// - `{{ asset.name }}` — the Asset's `metadata.name` (Origin-prefixed, e.g. `origin.model`).
///   Use for Nagi-internal references.
/// - `{{ asset.modelName }}` — the original model name without the Origin prefix
///   (e.g. `model`). Falls back to `asset.name` when unset.
///   Use for external tool arguments (e.g. `dbt run --select`).
/// - `{{ sync.<key> }}` — value from the `onDrift[].with` map.
fn expand_step(
    step: &SyncStep,
    asset_name: &str,
    model_name: &str,
    with: &HashMap<String, String>,
) -> SyncStep {
    SyncStep {
        step_type: step.step_type.clone(),
        args: step
            .args
            .iter()
            .map(|arg| expand_template_string(arg, asset_name, model_name, with))
            .collect(),
        env: step.env.clone(),
        identity: step.identity.clone(),
    }
}

fn expand_sync_templates(
    sync_spec: &SyncSpec,
    asset_name: &str,
    model_name: &str,
    with: &HashMap<String, String>,
) -> SyncSpec {
    SyncSpec {
        pre: sync_spec
            .pre
            .as_ref()
            .map(|s| expand_step(s, asset_name, model_name, with)),
        run: expand_step(&sync_spec.run, asset_name, model_name, with),
        post: sync_spec
            .post
            .as_ref()
            .map(|s| expand_step(s, asset_name, model_name, with)),
        identity: sync_spec.identity.clone(),
    }
}

/// Expands template variables in a DesiredCondition's args.
fn expand_condition_templates(
    condition: &DesiredCondition,
    asset_name: &str,
    model_name: &str,
    with: &HashMap<String, String>,
) -> DesiredCondition {
    match condition {
        DesiredCondition::Command {
            name,
            run,
            interval,
            env,
            evaluate_cache_ttl,
            identity,
        } => DesiredCondition::Command {
            name: name.clone(),
            run: run
                .iter()
                .map(|arg| expand_template_string(arg, asset_name, model_name, with))
                .collect(),
            interval: interval.clone(),
            env: env.clone(),
            evaluate_cache_ttl: evaluate_cache_ttl.clone(),
            identity: identity.clone(),
        },
        other => other.clone(),
    }
}

fn expand_template_string(
    s: &str,
    asset_name: &str,
    model_name: &str,
    with: &HashMap<String, String>,
) -> String {
    let mut result = s.replace("{{ asset.name }}", asset_name);
    result = result.replace("{{ asset.modelName }}", model_name);
    for (key, value) in with {
        result = result.replace(&format!("{{{{ sync.{key} }}}}"), value);
    }
    result
}

fn warn_multi_asset_sync(name: &str, spec: &SyncSpec) {
    let steps = [Some(&spec.run), spec.pre.as_ref(), spec.post.as_ref()];
    if let Some(reason) = steps
        .into_iter()
        .flatten()
        .find_map(|step| dbt::detect_multi_asset_step(&step.args))
    {
        tracing::warn!(
            sync = name,
            "Sync '{}' {}: this conflicts with Nagi's per-Asset reconciliation loop",
            name,
            reason,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ReadinessWarning {
    NoOnDrift,
    NoEvalTriggers,
}

fn check_readiness_warning(asset: &ResolvedAsset) -> Option<ReadinessWarning> {
    if asset.spec.on_drift.is_empty() {
        return Some(ReadinessWarning::NoOnDrift);
    }
    if asset.spec.upstreams.is_empty()
        && !asset
            .resolved_on_drift
            .iter()
            .flat_map(|e| &e.conditions)
            .any(|c| c.interval().is_some())
    {
        return Some(ReadinessWarning::NoEvalTriggers);
    }
    None
}

fn warn_asset_readiness(assets: &[ResolvedAsset]) {
    for a in assets {
        match check_readiness_warning(a) {
            Some(ReadinessWarning::NoOnDrift) => {
                tracing::warn!(
                    asset = %a.metadata.name,
                    "Asset '{}' has no onDrift entries: it will always be considered Ready",
                    a.metadata.name,
                );
            }
            Some(ReadinessWarning::NoEvalTriggers) => {
                tracing::warn!(
                    asset = %a.metadata.name,
                    "Asset '{}' has no evaluation triggers (no interval, no upstreams): after initial evaluation in serve, its state will not change",
                    a.metadata.name,
                );
            }
            None => {}
        }
    }
}

/// Resolves all references and builds the dependency graph.
pub fn resolve(resources: Vec<NagiKind>) -> Result<CompileOutput, CompileError> {
    let mut categorized = categorize(resources)?;

    categorized
        .syncs
        .iter()
        .for_each(|(name, spec)| warn_multi_asset_sync(name, spec));

    let assets = std::mem::take(&mut categorized.assets);
    let asset_names: HashSet<String> = assets.iter().map(|(m, _)| m.name.clone()).collect();

    let mut errors = validate_identity_refs(&categorized);
    warn_unsupported_identity_connections(&categorized);

    let mut resolved_assets = Vec::new();
    for (metadata, spec) in assets {
        match resolve_asset(metadata, spec, &asset_names, &categorized) {
            Ok(asset) => resolved_assets.push(asset),
            Err(e) => errors.push(e),
        }
    }

    warn_asset_readiness(&resolved_assets);

    let graph = build_graph(&resolved_assets)?;
    errors.extend(collect_cycle_errors(&graph));

    into_result(errors)?;

    Ok(CompileOutput {
        assets: resolved_assets,
        graph,
    })
}

/// Validates references and resolves a single Asset's on_drift entries and connection.
fn resolve_asset(
    metadata: Metadata,
    spec: AssetSpec,
    asset_names: &HashSet<String>,
    resources: &CategorizedResources,
) -> Result<ResolvedAsset, CompileError> {
    let CategorizedResources {
        connections,
        conditions_groups,
        syncs,
        identities,
        ..
    } = resources;
    let mut errors: Vec<CompileError> = Vec::new();

    errors.extend(collect_unresolved_upstream_errors(
        &spec.upstreams,
        asset_names,
    ));

    let model_name = spec
        .model_name
        .as_deref()
        .unwrap_or(&metadata.name)
        .to_string();
    let resolved_on_drift = match resolve_on_drift(
        &metadata.name,
        &model_name,
        &spec.on_drift,
        conditions_groups,
        syncs,
    ) {
        Ok(v) => v,
        Err(e) => {
            errors.push(e);
            Vec::new()
        }
    };
    errors.extend(
        resolved_on_drift
            .iter()
            .flat_map(|entry| &entry.conditions)
            .filter_map(|c| match c {
                DesiredCondition::Command {
                    identity: Some(id), ..
                } => Some(id),
                _ => None,
            })
            .filter(|id| !identities.contains_key(*id))
            .map(|id| CompileError::UnresolvedRef {
                kind: "Identity".to_string(),
                name: id.clone(),
            }),
    );

    let connection = match spec
        .connection
        .as_deref()
        .map(|name| resolve_connection_by_name(name, connections, identities))
        .transpose()
    {
        Ok(v) => v,
        Err(e) => {
            errors.push(e);
            None
        }
    };

    into_result(errors)?;

    Ok(ResolvedAsset {
        metadata,
        model_name,
        spec,
        resolved_on_drift,
        connection,
        dbt_cloud_job_ids: None,
    })
}

fn collect_unresolved_upstream_errors(
    upstreams: &[String],
    asset_names: &HashSet<String>,
) -> Vec<CompileError> {
    upstreams
        .iter()
        .filter(|name| !asset_names.contains(*name))
        .map(|name| CompileError::UnresolvedRef {
            kind: "Asset".to_string(),
            name: name.clone(),
        })
        .collect()
}

/// Validates that all Identity references in Syncs and Connections point to existing Identities.
fn validate_identity_refs(resources: &CategorizedResources) -> Vec<CompileError> {
    let identities = &resources.identities;

    let sync_refs = resources.syncs.values().flat_map(|spec| {
        [
            spec.identity.as_deref(),
            Some(&spec.run).and_then(|s| s.identity.as_deref()),
            spec.pre.as_ref().and_then(|s| s.identity.as_deref()),
            spec.post.as_ref().and_then(|s| s.identity.as_deref()),
        ]
        .into_iter()
        .flatten()
    });

    let connection_refs = resources
        .connections
        .values()
        .filter_map(|spec| connection_identity_ref(spec));

    sync_refs
        .chain(connection_refs)
        .filter(|id| !identities.contains_key(*id))
        .map(|id| CompileError::UnresolvedRef {
            kind: "Identity".to_string(),
            name: id.to_string(),
        })
        .collect()
}

fn warn_unsupported_identity_connections(resources: &CategorizedResources) {
    resources
        .connections
        .iter()
        .filter(|(_, spec)| {
            matches!(
                spec,
                ConnectionSpec::DuckDb { .. } | ConnectionSpec::Snowflake { .. }
            )
        })
        .filter_map(|(name, spec)| connection_identity_ref(spec).map(|id| (name, id)))
        .for_each(|(conn_name, id)| {
            tracing::warn!(
                connection = conn_name,
                identity = id,
                "identity on DuckDB/Snowflake Connection is not yet supported and will be ignored"
            );
        });
}

/// Resolves on_drift entries: validates conditions/sync refs and expands templates.
fn resolve_on_drift(
    asset_name: &str,
    model_name: &str,
    on_drift: &[asset::OnDriftEntry],
    conditions_groups: &HashMap<String, Vec<DesiredCondition>>,
    syncs: &HashMap<String, SyncSpec>,
) -> Result<Vec<ResolvedOnDriftEntry>, CompileError> {
    let mut resolved = Vec::new();
    let mut all_conditions: Vec<DesiredCondition> = Vec::new();
    let mut errors: Vec<CompileError> = Vec::new();

    for entry in on_drift {
        let conditions = match conditions_groups.get(&entry.conditions) {
            Some(c) => c,
            None => {
                errors.push(CompileError::UnresolvedRef {
                    kind: "Conditions".to_string(),
                    name: entry.conditions.clone(),
                });
                continue;
            }
        };

        if let Err(e) = require_sync_ref(syncs, &entry.sync) {
            errors.push(e);
            continue;
        }
        let sync_spec = &syncs[&entry.sync];
        let resolved_sync = expand_sync_templates(sync_spec, asset_name, model_name, &entry.with);

        let expanded_conditions: Vec<DesiredCondition> = conditions
            .iter()
            .map(|c| expand_condition_templates(c, asset_name, model_name, &entry.with))
            .collect();
        all_conditions.extend(expanded_conditions.clone());
        resolved.push(ResolvedOnDriftEntry {
            conditions: expanded_conditions,
            conditions_ref: entry.conditions.clone(),
            sync: resolved_sync,
            sync_ref_name: entry.sync.clone(),
        });
    }

    if let Err(e) = validate_no_duplicate_condition_names(&all_conditions) {
        errors.push(CompileError::Kind(e));
    }

    into_result(errors)?;
    Ok(resolved)
}

fn build_graph(assets: &[ResolvedAsset]) -> Result<DependencyGraph, CompileError> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for asset in assets {
        nodes.push(GraphNode {
            name: asset.metadata.name.clone(),
            kind: "Asset".to_string(),
            labels: asset.metadata.labels.clone(),
        });
        for upstream in &asset.spec.upstreams {
            edges.push(GraphEdge {
                from: upstream.clone(),
                to: asset.metadata.name.clone(),
            });
        }
    }

    nodes.sort_by(|a, b| a.name.cmp(&b.name));
    edges.sort_by(|a, b| (&a.from, &a.to).cmp(&(&b.from, &b.to)));

    Ok(DependencyGraph { nodes, edges })
}

/// Collects all nodes involved in dependency cycles using Kahn's algorithm.
fn collect_cycle_errors(graph: &DependencyGraph) -> Vec<CompileError> {
    let mut in_degree: HashMap<&str, usize> =
        graph.nodes.iter().map(|n| (n.name.as_str(), 0)).collect();
    let mut adjacency: HashMap<&str, Vec<&str>> = graph
        .nodes
        .iter()
        .map(|n| (n.name.as_str(), vec![]))
        .collect();

    for edge in &graph.edges {
        adjacency
            .get_mut(edge.from.as_str())
            .unwrap()
            .push(&edge.to);
        *in_degree.get_mut(edge.to.as_str()).unwrap() += 1;
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&name, _)| name)
        .collect();

    while let Some(node) = queue.pop_front() {
        for &neighbor in &adjacency[node] {
            let deg = in_degree.get_mut(neighbor).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(neighbor);
            }
        }
    }

    let mut cycle_nodes: Vec<_> = in_degree
        .into_iter()
        .filter(|(_, deg)| *deg > 0)
        .map(|(name, _)| name.to_string())
        .collect();
    cycle_nodes.sort();
    cycle_nodes
        .into_iter()
        .map(|name| CompileError::CycleDetected { name })
        .collect()
}

/// Serialization-only struct for writing compiled assets to `target/`.
/// Embeds resolved SyncSpec directly instead of SyncRef.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompiledAssetYaml<'a> {
    api_version: &'static str,
    kind: &'static str,
    metadata: &'a Metadata,
    spec: CompiledAssetSpecYaml<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connection: &'a Option<ResolvedConnection>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompiledAssetSpecYaml<'a> {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    upstreams: &'a Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    on_drift: &'a Vec<ResolvedOnDriftEntry>,
    auto_sync: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    dbt_cloud_job_ids: &'a Option<HashSet<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    evaluate_cache_ttl: &'a Option<crate::runtime::duration::Duration>,
    model_name: &'a str,
}

/// Deserialization struct for reading compiled asset YAML from `target/`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledAsset {
    #[serde(rename = "apiVersion")]
    pub _api_version: String,
    pub metadata: Metadata,
    pub spec: CompiledAssetSpec,
    #[serde(default)]
    pub connection: Option<ResolvedConnection>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledAssetSpec {
    #[serde(default)]
    pub upstreams: Vec<String>,
    #[serde(default)]
    pub on_drift: Vec<ResolvedOnDriftEntry>,
    #[serde(default = "default_true")]
    pub auto_sync: bool,
    /// dbt Cloud job IDs that include this asset in their execute_steps.
    /// Resolved at compile time. Used for running-job checks before sync.
    #[serde(default)]
    pub dbt_cloud_job_ids: Option<HashSet<i64>>,
    /// Asset-level default evaluate cache TTL.
    #[serde(default, rename = "evaluateCacheTtl")]
    pub evaluate_cache_ttl: Option<crate::runtime::duration::Duration>,
    /// Original model name without the Origin prefix.
    #[serde(default)]
    #[allow(dead_code)]
    pub model_name: Option<String>,
}

fn default_true() -> bool {
    true
}

pub fn write_output(output: &CompileOutput, target_dir: &Path) -> Result<(), CompileError> {
    let assets_dir = target_dir.join("assets");
    std::fs::create_dir_all(&assets_dir)?;

    for asset in &output.assets {
        let compiled = CompiledAssetYaml {
            api_version: kind::API_VERSION,
            kind: "Asset",
            metadata: &asset.metadata,
            spec: CompiledAssetSpecYaml {
                upstreams: &asset.spec.upstreams,
                on_drift: &asset.resolved_on_drift,
                auto_sync: asset.spec.auto_sync,
                dbt_cloud_job_ids: &asset.dbt_cloud_job_ids,
                evaluate_cache_ttl: &asset.spec.evaluate_cache_ttl,
                model_name: &asset.model_name,
            },
            connection: &asset.connection,
        };
        let yaml = serde_yaml::to_string(&compiled).map_err(KindError::YamlParse)?;
        std::fs::write(
            assets_dir.join(format!("{}.yaml", asset.metadata.name)),
            yaml,
        )?;
    }

    let graph_json = serde_json::to_string_pretty(&output.graph).map_err(std::io::Error::other)?;
    std::fs::write(target_dir.join("graph.json"), graph_json)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::kind::asset::OnDriftEntry;
    use crate::runtime::kind::parse_kinds;
    use tempfile::TempDir;

    // ── YAML fragments ──────────────────────────────────────────────────

    const CONNECTION_MY_BQ: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bq
spec:
  type: dbt
  profile: my_project";

    const ASSET_RAW_SALES: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: raw-sales
spec:
  connection: my-bq";

    const SYNC_DBT_RUN: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-run
spec:
  run:
    type: Command
    args: [\"dbt\", \"run\", \"--select\", \"{{ asset.name }}\"]";

    const SYNC_DBT_FULL: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-full
spec:
  run:
    type: Command
    args: [\"dbt\", \"run\", \"--full-refresh\", \"--select\", \"{{ asset.name }}\"]";

    const DESIRED_GROUP_DAILY_SLA: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: daily-sla
spec:
  - name: freshness-24h
    type: Freshness
    maxAge: 24h
    interval: 6h";

    /// Joins YAML documents with `---` separator.
    fn yaml_docs(docs: &[&str]) -> String {
        docs.join("\n---\n")
    }

    fn write_yaml(dir: &Path, filename: &str, content: &str) {
        std::fs::write(dir.join(filename), content).unwrap();
    }

    fn parse(yaml: &str) -> Vec<NagiKind> {
        parse_kinds(yaml).unwrap()
    }

    // ── check_readiness_warning tests ──────────────────────────────────

    fn on_drift_entry() -> OnDriftEntry {
        OnDriftEntry {
            conditions: "cond".to_string(),
            sync: "sync".to_string(),
            with: HashMap::new(),
            merge_position: Default::default(),
        }
    }

    fn make_resolved_asset(
        name: &str,
        upstreams: Vec<String>,
        on_drift: Vec<OnDriftEntry>,
        resolved_on_drift: Vec<ResolvedOnDriftEntry>,
    ) -> ResolvedAsset {
        ResolvedAsset {
            metadata: Metadata::new(name),
            model_name: name.to_string(),
            spec: AssetSpec {
                connection: None,
                upstreams,
                on_drift,
                auto_sync: true,
                evaluate_cache_ttl: None,
                model_name: None,
            },
            resolved_on_drift,
            connection: None,
            dbt_cloud_job_ids: None,
        }
    }

    fn sample_sync_spec() -> SyncSpec {
        SyncSpec {
            pre: None,
            run: crate::runtime::kind::sync::SyncStep {
                step_type: crate::runtime::kind::sync::StepType::Command,
                args: vec!["true".to_string()],
                env: HashMap::new(),
                identity: None,
            },
            post: None,
            identity: None,
        }
    }

    fn resolved_entry_with_conditions(conditions: Vec<DesiredCondition>) -> ResolvedOnDriftEntry {
        ResolvedOnDriftEntry {
            conditions,
            conditions_ref: "cond".to_string(),
            sync: sample_sync_spec(),
            sync_ref_name: "sync".to_string(),
        }
    }

    macro_rules! check_readiness_warning_test {
        ($($name:ident: $asset:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let asset = $asset;
                    assert_eq!(check_readiness_warning(&asset), $expected);
                }
            )*
        };
    }

    check_readiness_warning_test! {
        empty_on_drift_warns_no_on_drift: make_resolved_asset(
            "a", vec![], vec![], vec![],
        ) => Some(ReadinessWarning::NoOnDrift);

        no_interval_no_upstreams_warns_no_eval_triggers: make_resolved_asset(
            "a",
            vec![],
            vec![on_drift_entry()],
            vec![resolved_entry_with_conditions(vec![DesiredCondition::Sql {
                name: "sql-check".to_string(),
                query: "SELECT 1".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
            }])],
        ) => Some(ReadinessWarning::NoEvalTriggers);

        has_interval_no_warning: make_resolved_asset(
            "a",
            vec![],
            vec![on_drift_entry()],
            vec![resolved_entry_with_conditions(vec![DesiredCondition::Freshness {
                name: "freshness".to_string(),
                max_age: crate::runtime::duration::Duration::from_secs(86400),
                column: None,
                interval: crate::runtime::duration::Duration::from_secs(3600),
                check_at: None,
                evaluate_cache_ttl: None,
            }])],
        ) => None;

        has_upstreams_no_warning: make_resolved_asset(
            "a",
            vec!["upstream-asset".to_string()],
            vec![on_drift_entry()],
            vec![resolved_entry_with_conditions(vec![DesiredCondition::Sql {
                name: "sql-check".to_string(),
                query: "SELECT 1".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
            }])],
        ) => None;
    }

    // ── resolve_on_drift tests ──────────────────────────────────────────

    fn sample_conditions() -> HashMap<String, Vec<DesiredCondition>> {
        HashMap::from([(
            "daily-sla".to_string(),
            vec![DesiredCondition::Freshness {
                name: "freshness-24h".to_string(),
                max_age: crate::runtime::duration::Duration::from_secs(86400),
                column: None,
                interval: crate::runtime::duration::Duration::from_secs(21600),
                check_at: None,
                evaluate_cache_ttl: None,
            }],
        )])
    }

    fn sample_syncs() -> HashMap<String, SyncSpec> {
        HashMap::from([(
            "dbt-run".to_string(),
            SyncSpec {
                pre: None,
                run: crate::runtime::kind::sync::SyncStep {
                    step_type: crate::runtime::kind::sync::StepType::Command,
                    args: vec![
                        "dbt".to_string(),
                        "run".to_string(),
                        "--select".to_string(),
                        "{{ asset.name }}".to_string(),
                    ],
                    env: HashMap::new(),
                    identity: None,
                },
                post: None,
                identity: None,
            },
        )])
    }

    #[test]
    fn resolve_on_drift_empty() {
        let result = resolve_on_drift("a", "a", &[], &HashMap::new(), &HashMap::new()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn resolve_on_drift_expands_conditions_and_templates() {
        let entry = asset::OnDriftEntry {
            conditions: "daily-sla".to_string(),
            sync: "dbt-run".to_string(),
            with: HashMap::new(),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let result = resolve_on_drift(
            "daily-sales",
            "daily-sales",
            &[entry],
            &sample_conditions(),
            &sample_syncs(),
        )
        .unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(
            &result[0].conditions[0],
            DesiredCondition::Freshness { .. }
        ));
        assert_eq!(result[0].sync.run.args[3], "daily-sales");
    }

    #[test]
    fn resolve_on_drift_rejects_missing_conditions_ref() {
        let entry = asset::OnDriftEntry {
            conditions: "nonexistent".to_string(),
            sync: "dbt-run".to_string(),
            with: HashMap::new(),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let err =
            resolve_on_drift("a", "a", &[entry], &HashMap::new(), &sample_syncs()).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, .. } if kind == "Conditions"));
    }

    #[test]
    fn resolve_on_drift_rejects_missing_sync_ref() {
        let entry = asset::OnDriftEntry {
            conditions: "daily-sla".to_string(),
            sync: "nonexistent".to_string(),
            with: HashMap::new(),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let err = resolve_on_drift("a", "a", &[entry], &sample_conditions(), &HashMap::new())
            .unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, .. } if kind == "Sync"));
    }

    #[test]
    fn resolve_on_drift_rejects_duplicate_condition_names() {
        let conditions = HashMap::from([
            (
                "group-a".to_string(),
                vec![DesiredCondition::Command {
                    name: "dup-name".to_string(),
                    run: vec!["true".to_string()],
                    interval: None,
                    env: HashMap::new(),
                    evaluate_cache_ttl: None,
                    identity: None,
                }],
            ),
            (
                "group-b".to_string(),
                vec![DesiredCondition::Command {
                    name: "dup-name".to_string(),
                    run: vec!["true".to_string()],
                    interval: None,
                    env: HashMap::new(),
                    evaluate_cache_ttl: None,
                    identity: None,
                }],
            ),
        ]);
        let entries = vec![
            asset::OnDriftEntry {
                conditions: "group-a".to_string(),
                sync: "dbt-run".to_string(),
                with: HashMap::new(),
                merge_position: asset::MergePosition::BeforeOrigin,
            },
            asset::OnDriftEntry {
                conditions: "group-b".to_string(),
                sync: "dbt-run".to_string(),
                with: HashMap::new(),
                merge_position: asset::MergePosition::BeforeOrigin,
            },
        ];
        let err = resolve_on_drift("a", "a", &entries, &conditions, &sample_syncs()).unwrap_err();
        assert!(matches!(err, CompileError::Kind(_)));
    }

    #[test]
    fn resolve_on_drift_with_variables() {
        let syncs = HashMap::from([(
            "dbt-run".to_string(),
            SyncSpec {
                pre: None,
                run: crate::runtime::kind::sync::SyncStep {
                    step_type: crate::runtime::kind::sync::StepType::Command,
                    args: vec![
                        "dbt".to_string(),
                        "run".to_string(),
                        "--select".to_string(),
                        "{{ sync.selector }}".to_string(),
                    ],
                    env: HashMap::new(),
                    identity: None,
                },
                post: None,
                identity: None,
            },
        )]);
        let entry = asset::OnDriftEntry {
            conditions: "daily-sla".to_string(),
            sync: "dbt-run".to_string(),
            with: HashMap::from([("selector".to_string(), "+daily_sales".to_string())]),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let result = resolve_on_drift(
            "daily-sales",
            "daily-sales",
            &[entry],
            &sample_conditions(),
            &syncs,
        )
        .unwrap();
        assert_eq!(result[0].sync.run.args[3], "+daily_sales");
    }

    #[test]
    fn resolve_on_drift_expands_conditions_templates() {
        let conditions = HashMap::from([(
            "export-drift".to_string(),
            vec![DesiredCondition::Command {
                name: "unexported-rows".to_string(),
                run: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "nagi export --select {{ sync.table }} --dry-run".to_string(),
                ],
                interval: None,
                env: HashMap::new(),
                evaluate_cache_ttl: None,
                identity: None,
            }],
        )]);
        let entry = asset::OnDriftEntry {
            conditions: "export-drift".to_string(),
            sync: "dbt-run".to_string(),
            with: HashMap::from([("table".to_string(), "evaluate_logs".to_string())]),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let result = resolve_on_drift(
            "nagi-export-evaluate_logs",
            "nagi-export-evaluate_logs",
            &[entry],
            &conditions,
            &sample_syncs(),
        )
        .unwrap();
        if let DesiredCondition::Command { run, .. } = &result[0].conditions[0] {
            assert_eq!(run[2], "nagi export --select evaluate_logs --dry-run");
        } else {
            panic!("expected Command condition");
        }
    }

    #[test]
    fn resolve_on_drift_expands_model_name_template() {
        let syncs = HashMap::from([(
            "dbt-run".to_string(),
            SyncSpec {
                pre: None,
                run: crate::runtime::kind::sync::SyncStep {
                    step_type: crate::runtime::kind::sync::StepType::Command,
                    args: vec![
                        "dbt".to_string(),
                        "run".to_string(),
                        "--select".to_string(),
                        "{{ asset.modelName }}".to_string(),
                    ],
                    env: HashMap::new(),
                    identity: None,
                },
                post: None,
                identity: None,
            },
        )]);
        // Origin-generated Asset: model_name differs from asset name
        let entry = asset::OnDriftEntry {
            conditions: "daily-sla".to_string(),
            sync: "dbt-run".to_string(),
            with: HashMap::new(),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let result = resolve_on_drift(
            "my-dbt.orders",
            "orders",
            &[entry],
            &sample_conditions(),
            &syncs,
        )
        .unwrap();
        assert_eq!(
            result[0].sync.run.args[3], "orders",
            "{{ asset.modelName }} should expand to the original model name"
        );
    }

    // ── resolve (integration) tests ─────────────────────────────────────

    #[test]
    fn resolve_minimal_asset() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}",
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        assert_eq!(output.assets[0].metadata.name, "daily-sales");
        assert_eq!(output.graph.nodes.len(), 1);
        assert!(output.graph.edges.is_empty());
    }

    #[test]
    fn resolve_rejects_unresolved_upstream_ref() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  upstreams:
    - nonexistent-asset",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Asset" && name == "nonexistent-asset"));
    }

    #[test]
    fn resolve_merges_duplicate_asset_on_drift() {
        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: quality-checks
spec:
  - name: check-b
    type: SQL
    query: \"SELECT true\"",
            SYNC_DBT_RUN,
            SYNC_DBT_FULL,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
  labels:
    dbt/finance: ''
spec:
  onDrift:
    - conditions: daily-sla
      sync: dbt-run
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
  labels:
    dbt/other: ''
spec:
  onDrift:
    - conditions: quality-checks
      sync: dbt-full",
        ]));
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        let asset = &output.assets[0];
        assert_eq!(asset.metadata.name, "daily-sales");
        assert_eq!(asset.resolved_on_drift.len(), 2);
        // Merge preserves first asset's labels, not the overlay's.
        assert_eq!(
            asset.metadata.labels.get("dbt/finance"),
            Some(&String::new())
        );
        assert_eq!(asset.metadata.labels.get("dbt/other"), None);
    }

    #[test]
    fn resolve_rejects_triple_duplicate_asset() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::DuplicateName { kind, name }
            if kind == "Asset" && name == "daily-sales"));
    }

    #[test]
    fn resolve_merge_orders_by_merge_position() {
        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: quality-checks
spec:
  - name: check-b
    type: SQL
    query: \"SELECT true\"",
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: post-checks
spec:
  - name: check-c
    type: SQL
    query: \"SELECT 1\"",
            SYNC_DBT_RUN,
            SYNC_DBT_FULL,
            "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-post
spec:
  run:
    type: Command
    args: [\"dbt\", \"test\"]",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: daily-sla
      sync: dbt-run
      mergePosition: beforeOrigin
    - conditions: post-checks
      sync: dbt-post
      mergePosition: afterOrigin
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: quality-checks
      sync: dbt-full",
        ]));
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        let on_drift = &output.assets[0].resolved_on_drift;
        assert_eq!(on_drift.len(), 3);
        // [beforeOrigin user entry] + [origin entry] + [afterOrigin user entry]
        assert_eq!(on_drift[0].conditions_ref, "daily-sla");
        assert_eq!(on_drift[1].conditions_ref, "quality-checks");
        assert_eq!(on_drift[2].conditions_ref, "post-checks");
    }

    #[test]
    fn resolve_merge_default_position_is_before_origin() {
        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: quality-checks
spec:
  - name: check-b
    type: SQL
    query: \"SELECT true\"",
            SYNC_DBT_RUN,
            SYNC_DBT_FULL,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: daily-sla
      sync: dbt-run
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: quality-checks
      sync: dbt-full",
        ]));
        let output = resolve(resources).unwrap();
        let on_drift = &output.assets[0].resolved_on_drift;
        assert_eq!(on_drift.len(), 2);
        // Default mergePosition is beforeOrigin, so user entry comes first
        assert_eq!(on_drift[0].conditions_ref, "daily-sla");
        assert_eq!(on_drift[1].conditions_ref, "quality-checks");
    }

    #[test]
    fn resolve_builds_dependency_graph() {
        let resources = parse(&yaml_docs(&[
            CONNECTION_MY_BQ,
            ASSET_RAW_SALES,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
  labels:
    dbt/finance: ''
spec:
  upstreams:
    - raw-sales",
        ]));
        let output = resolve(resources).unwrap();
        assert_eq!(output.graph.nodes.len(), 2);

        let asset_node = output
            .graph
            .nodes
            .iter()
            .find(|n| n.name == "daily-sales")
            .unwrap();
        assert_eq!(asset_node.labels.get("dbt/finance"), Some(&String::new()));

        assert_eq!(output.graph.edges.len(), 1);
        assert_eq!(output.graph.edges[0].from, "raw-sales");
        assert_eq!(output.graph.edges[0].to, "daily-sales");
    }

    #[test]
    fn resolve_asset_chain_dependency() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: raw
spec: {}
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: staging
spec:
  upstreams: [raw]
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: mart
spec:
  upstreams: [staging]",
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 3);
        assert_eq!(output.graph.nodes.len(), 3);
        assert!(output.graph.nodes.iter().all(|n| n.kind == "Asset"));
        assert_eq!(output.graph.edges.len(), 2);

        let edge_pairs: Vec<(&str, &str)> = output
            .graph
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        assert!(edge_pairs.contains(&("raw", "staging")));
        assert!(edge_pairs.contains(&("staging", "mart")));
    }

    #[test]
    fn resolve_diamond_dependency() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: root
spec: {}
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: left
spec:
  upstreams: [root]
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: right
spec:
  upstreams: [root]
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: sink
spec:
  upstreams: [left, right]",
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 4);
        assert_eq!(output.graph.edges.len(), 4);

        let edge_pairs: Vec<(&str, &str)> = output
            .graph
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        assert!(edge_pairs.contains(&("root", "left")));
        assert!(edge_pairs.contains(&("root", "right")));
        assert!(edge_pairs.contains(&("left", "sink")));
        assert!(edge_pairs.contains(&("right", "sink")));
    }

    // ── write_output tests ────────────────────────────────────────────────

    #[test]
    fn write_output_creates_asset_yaml() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");

        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}",
        );
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let yaml_path = target.join("assets/daily-sales.yaml");
        assert!(yaml_path.exists());

        let content = std::fs::read_to_string(&yaml_path).unwrap();
        let kinds = parse_kinds(&content).unwrap();
        assert_eq!(kinds.len(), 1);
        assert!(
            matches!(&kinds[0], NagiKind::Asset { metadata, .. } if metadata.name == "daily-sales")
        );
    }

    #[test]
    fn write_output_creates_graph_json() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");

        let resources = parse(&yaml_docs(&[
            CONNECTION_MY_BQ,
            ASSET_RAW_SALES,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  upstreams:
    - raw-sales",
        ]));
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let graph_content = std::fs::read_to_string(target.join("graph.json")).unwrap();
        let graph: DependencyGraph = serde_json::from_str(&graph_content).unwrap();
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].from, "raw-sales");
        assert_eq!(graph.edges[0].to, "daily-sales");
    }

    #[test]
    fn write_output_embeds_resolved_on_drift_sync() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");

        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            SYNC_DBT_RUN,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: daily-sla
      sync: dbt-run",
        ]));
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let content = std::fs::read_to_string(target.join("assets/daily-sales.yaml")).unwrap();
        let value: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let sync_args = &value["spec"]["onDrift"][0]["sync"]["run"]["args"];
        let args: Vec<String> = serde_yaml::from_value(sync_args.clone()).unwrap();
        assert_eq!(args, vec!["dbt", "run", "--select", "daily-sales"]);
        assert!(!target.join("syncs").exists());
    }

    // ── expand_origins tests ────────────────────────────────────────────

    const MANIFEST_JSON: &str = r#"{
  "nodes": {
    "model.shop.stg_customers": {
      "unique_id": "model.shop.stg_customers",
      "resource_type": "model",
      "name": "stg_customers",
      "package_name": "shop",
      "tags": [],
      "depends_on": { "nodes": ["source.shop.raw.customers"] }
    },
    "model.shop.customers": {
      "unique_id": "model.shop.customers",
      "resource_type": "model",
      "name": "customers",
      "package_name": "shop",
      "tags": ["finance"],
      "depends_on": { "nodes": ["model.shop.stg_customers"] }
    },
    "test.shop.not_null_customers_id.abc": {
      "unique_id": "test.shop.not_null_customers_id.abc",
      "resource_type": "test",
      "name": "not_null_customers_id",
      "package_name": "shop",
      "tags": [],
      "depends_on": { "nodes": ["model.shop.customers"] },
      "test_metadata": { "name": "not_null", "kwargs": { "column_name": "id" } }
    }
  },
  "sources": {
    "source.shop.raw.customers": {
      "unique_id": "source.shop.raw.customers",
      "name": "customers",
      "source_name": "raw"
    }
  }
}"#;

    const ORIGIN_YAML: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: my-dbt
spec:
  type: DBT
  connection: my-bq
  projectDir: ../dbt-project
  defaultSync:
    sync: dbt-run";

    fn manifests_for(origin_name: &str) -> HashMap<String, String> {
        HashMap::from([(origin_name.to_string(), MANIFEST_JSON.to_string())])
    }

    #[test]
    fn expand_origins_generates_resources_from_manifest() {
        let resources = parse(&yaml_docs(&[CONNECTION_MY_BQ, SYNC_DBT_RUN, ORIGIN_YAML]));
        let manifests = manifests_for("my-dbt");
        let expanded = crate::runtime::kind::origin::dbt::generate::generate_with_manifests(
            resources, &manifests, None,
        )
        .unwrap();

        let assets: Vec<_> = expanded.iter().filter(|r| r.kind() == "Asset").collect();
        // 1 dbt source Asset + 2 model Assets
        assert_eq!(assets.len(), 3);

        let syncs: Vec<_> = expanded.iter().filter(|r| r.kind() == "Sync").collect();
        // dbt-run (user-defined only; tag-based syncs are no longer auto-generated)
        assert_eq!(syncs.len(), 1);
    }

    #[test]
    fn expand_origins_noop_without_origin() {
        let resources = parse(&yaml_docs(&[CONNECTION_MY_BQ, ASSET_RAW_SALES]));
        let count = resources.len();
        let expanded = crate::runtime::kind::origin::dbt::generate::generate_with_manifests(
            resources,
            &HashMap::new(),
            None,
        )
        .unwrap();
        assert_eq!(expanded.len(), count);
    }

    #[test]
    fn expand_origins_error_when_no_manifest() {
        let resources = parse(ORIGIN_YAML);
        let err = crate::runtime::kind::origin::dbt::generate::generate_with_manifests(
            resources,
            &HashMap::new(),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, CompileError::ManifestParse(_)));
    }

    #[test]
    fn resolve_with_origin_expansion() {
        let resources = parse(&yaml_docs(&[CONNECTION_MY_BQ, SYNC_DBT_RUN, ORIGIN_YAML]));
        let manifests = manifests_for("my-dbt");
        let expanded = crate::runtime::kind::origin::dbt::generate::generate_with_manifests(
            resources, &manifests, None,
        )
        .unwrap();
        let output = resolve(expanded).unwrap();

        // 1 dbt source Asset + 2 model Assets
        assert_eq!(output.assets.len(), 3);
        let customer_asset = output
            .assets
            .iter()
            .find(|a| a.metadata.name == "my-dbt.customers")
            .unwrap();
        assert!(!customer_asset.resolved_on_drift.is_empty());

        // Verify model-to-model dependency edge exists (previously discarded).
        let edge_pairs: Vec<(&str, &str)> = output
            .graph
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        assert!(
            edge_pairs.contains(&("my-dbt.stg_customers", "my-dbt.customers")),
            "model-to-model dependency must produce a graph edge: edges = {edge_pairs:?}"
        );
        // raw.customers → stg_customers edge
        assert!(
            edge_pairs.contains(&("my-dbt.raw.customers", "my-dbt.stg_customers")),
            "upstream dependency must produce a graph edge: edges = {edge_pairs:?}"
        );
    }

    #[test]
    fn compile_with_origin_writes_target() {
        let tmp = TempDir::new().unwrap();
        let resources_dir = tmp.path().join("resources");
        let target_dir = tmp.path().join("nagi_target");
        std::fs::create_dir_all(&resources_dir).unwrap();

        write_yaml(
            &resources_dir,
            "infra.yaml",
            &yaml_docs(&[CONNECTION_MY_BQ, SYNC_DBT_RUN, ORIGIN_YAML]),
        );

        let resources = load_resources(&resources_dir).unwrap();
        let manifests = manifests_for("my-dbt");
        let resources = crate::runtime::kind::origin::dbt::generate::generate_with_manifests(
            resources, &manifests, None,
        )
        .unwrap();
        let output = resolve(resources).unwrap();
        write_output(&output, &target_dir).unwrap();

        assert!(target_dir.join("graph.json").exists());
        assert!(target_dir.join("assets/my-dbt.customers.yaml").exists());
        assert!(target_dir.join("assets/my-dbt.stg_customers.yaml").exists());
    }

    // ── expand_step ─────────────────────────────────────────────────────

    #[test]
    fn expand_step_replaces_templates_in_args() {
        let step = SyncStep {
            step_type: crate::runtime::kind::sync::StepType::Command,
            args: vec![
                "dbt".into(),
                "run".into(),
                "--select".into(),
                "{{ asset.name }}".into(),
            ],
            env: HashMap::new(),
            identity: None,
        };
        let result = expand_step(&step, "origin.model", "model", &HashMap::new());
        assert_eq!(result.args, vec!["dbt", "run", "--select", "origin.model"]);
    }

    #[test]
    fn expand_step_replaces_with_variables() {
        let step = SyncStep {
            step_type: crate::runtime::kind::sync::StepType::Command,
            args: vec!["{{ sync.target }}".into()],
            env: HashMap::new(),
            identity: None,
        };
        let mut with = HashMap::new();
        with.insert("target".into(), "prod".into());
        let result = expand_step(&step, "a", "b", &with);
        assert_eq!(result.args, vec!["prod"]);
    }

    // ── into_result ────────────────────────────────────────────────────

    #[test]
    fn into_result_empty_is_ok() {
        assert!(into_result(vec![]).is_ok());
    }

    #[test]
    fn into_result_single_returns_that_error() {
        let err = into_result(vec![CompileError::CycleDetected { name: "a".into() }]).unwrap_err();
        assert!(matches!(err, CompileError::CycleDetected { name } if name == "a"));
    }

    #[test]
    fn into_result_multiple_returns_multiple_variant() {
        let err = into_result(vec![
            CompileError::CycleDetected { name: "a".into() },
            CompileError::CycleDetected { name: "b".into() },
        ])
        .unwrap_err();
        match err {
            CompileError::Multiple(errors) => assert_eq!(errors.len(), 2),
            other => panic!("expected Multiple, got: {other}"),
        }
    }

    // ── collect_unresolved_upstream_errors ──────────────────────────────

    #[test]
    fn collect_unresolved_upstream_errors_all_valid() {
        let names: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let upstreams = vec!["a".to_string(), "b".to_string()];
        assert!(collect_unresolved_upstream_errors(&upstreams, &names).is_empty());
    }

    #[test]
    fn collect_unresolved_upstream_errors_multiple_missing() {
        let names: HashSet<String> = HashSet::new();
        let upstreams = vec!["x".to_string(), "y".to_string()];
        let errors = collect_unresolved_upstream_errors(&upstreams, &names);
        assert_eq!(errors.len(), 2);
        assert!(errors
            .iter()
            .all(|e| matches!(e, CompileError::UnresolvedRef { kind, .. } if kind == "Asset")));
    }

    #[test]
    fn collect_unresolved_upstream_errors_partial_match() {
        let names: HashSet<String> = ["a"].iter().map(|s| s.to_string()).collect();
        let upstreams = vec!["a".to_string(), "missing".to_string()];
        let errors = collect_unresolved_upstream_errors(&upstreams, &names);
        assert_eq!(errors.len(), 1);
        assert!(
            matches!(&errors[0], CompileError::UnresolvedRef { name, .. } if name == "missing")
        );
    }

    // ── collect_cycle_errors ───────────────────────────────────────────

    #[test]
    fn collect_cycle_errors_empty_graph() {
        let graph = DependencyGraph {
            nodes: vec![],
            edges: vec![],
        };
        assert!(collect_cycle_errors(&graph).is_empty());
    }

    #[test]
    fn collect_cycle_errors_no_cycle() {
        let graph = DependencyGraph {
            nodes: vec![
                GraphNode {
                    name: "a".into(),
                    kind: "Asset".into(),
                    labels: Default::default(),
                },
                GraphNode {
                    name: "b".into(),
                    kind: "Asset".into(),
                    labels: Default::default(),
                },
            ],
            edges: vec![GraphEdge {
                from: "a".into(),
                to: "b".into(),
            }],
        };
        assert!(collect_cycle_errors(&graph).is_empty());
    }

    #[test]
    fn collect_cycle_errors_reports_all_cycle_nodes() {
        let graph = DependencyGraph {
            nodes: vec![
                GraphNode {
                    name: "a".into(),
                    kind: "Asset".into(),
                    labels: Default::default(),
                },
                GraphNode {
                    name: "b".into(),
                    kind: "Asset".into(),
                    labels: Default::default(),
                },
                GraphNode {
                    name: "c".into(),
                    kind: "Asset".into(),
                    labels: Default::default(),
                },
            ],
            edges: vec![
                GraphEdge {
                    from: "a".into(),
                    to: "b".into(),
                },
                GraphEdge {
                    from: "b".into(),
                    to: "a".into(),
                },
            ],
        };
        let errors = collect_cycle_errors(&graph);
        assert_eq!(errors.len(), 2);
        let names: Vec<String> = errors
            .iter()
            .map(|e| match e {
                CompileError::CycleDetected { name } => name.clone(),
                _ => panic!("expected CycleDetected"),
            })
            .collect();
        assert!(names.contains(&"a".to_string()));
        assert!(names.contains(&"b".to_string()));
    }

    #[test]
    fn collect_cycle_errors_self_cycle() {
        let graph = DependencyGraph {
            nodes: vec![GraphNode {
                name: "a".into(),
                kind: "Asset".into(),
                labels: Default::default(),
            }],
            edges: vec![GraphEdge {
                from: "a".into(),
                to: "a".into(),
            }],
        };
        let errors = collect_cycle_errors(&graph);
        assert_eq!(errors.len(), 1);
        assert!(matches!(&errors[0], CompileError::CycleDetected { name } if name == "a"));
    }

    // ── resolve accumulates multiple errors ────────────────────────────

    #[test]
    fn resolve_accumulates_multiple_unresolved_refs() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: a
spec:
  upstreams: [missing-1, missing-2]",
        );
        let err = resolve(resources).unwrap_err();
        match err {
            CompileError::Multiple(errors) => {
                assert_eq!(errors.len(), 2);
                assert!(errors
                    .iter()
                    .all(|e| matches!(e, CompileError::UnresolvedRef { .. })));
            }
            other => panic!("expected Multiple, got: {other}"),
        }
    }

    #[test]
    fn resolve_accumulates_errors_across_assets() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: a
spec:
  upstreams: [nonexistent-1]
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: b
spec:
  upstreams: [nonexistent-2]",
        );
        let err = resolve(resources).unwrap_err();
        match err {
            CompileError::Multiple(errors) => {
                assert_eq!(errors.len(), 2);
            }
            other => panic!("expected Multiple, got: {other}"),
        }
    }

    #[test]
    fn resolve_on_drift_accumulates_missing_conditions_and_sync() {
        let entries = vec![
            asset::OnDriftEntry {
                conditions: "missing-cond".to_string(),
                sync: "dbt-run".to_string(),
                with: HashMap::new(),
                merge_position: asset::MergePosition::BeforeOrigin,
            },
            asset::OnDriftEntry {
                conditions: "daily-sla".to_string(),
                sync: "missing-sync".to_string(),
                with: HashMap::new(),
                merge_position: asset::MergePosition::BeforeOrigin,
            },
        ];
        let err = resolve_on_drift("a", "a", &entries, &sample_conditions(), &sample_syncs())
            .unwrap_err();
        match err {
            CompileError::Multiple(errors) => {
                assert_eq!(errors.len(), 2);
            }
            other => panic!("expected Multiple, got: {other}"),
        }
    }

    // ── validate_identity_refs ─────────────────────────────────────────

    fn empty_categorized() -> CategorizedResources {
        CategorizedResources {
            connections: HashMap::new(),
            conditions_groups: HashMap::new(),
            syncs: HashMap::new(),
            assets: Vec::new(),
            identities: HashMap::new(),
        }
    }

    #[test]
    fn validate_identity_refs_accepts_existing_identity_in_sync() {
        let mut resources = empty_categorized();
        resources.identities.insert(
            "bq-eval".to_string(),
            kind::identity::IdentitySpec::Env {
                env: HashMap::new(),
            },
        );
        resources.syncs.insert(
            "s".to_string(),
            SyncSpec {
                identity: Some("bq-eval".to_string()),
                ..SyncSpec::new(SyncStep::command(vec!["true".to_string()]))
            },
        );
        assert!(validate_identity_refs(&resources).is_empty());
    }

    #[test]
    fn validate_identity_refs_rejects_missing_identity_in_sync() {
        let mut resources = empty_categorized();
        resources.syncs.insert(
            "s".to_string(),
            SyncSpec {
                identity: Some("nonexistent".to_string()),
                ..SyncSpec::new(SyncStep::command(vec!["true".to_string()]))
            },
        );
        let errors = validate_identity_refs(&resources);
        assert_eq!(errors.len(), 1);
        assert!(
            matches!(&errors[0], CompileError::UnresolvedRef { kind, name }
            if kind == "Identity" && name == "nonexistent")
        );
    }

    #[test]
    fn validate_identity_refs_rejects_missing_identity_in_connection() {
        let mut resources = empty_categorized();
        resources.connections.insert(
            "c".to_string(),
            ConnectionSpec::BigQuery {
                project: "p".to_string(),
                dataset: "d".to_string(),
                execution_project: None,
                method: None,
                keyfile: None,
                timeout_seconds: None,
                identity: Some("nonexistent".to_string()),
            },
        );
        let errors = validate_identity_refs(&resources);
        assert_eq!(errors.len(), 1);
        assert!(
            matches!(&errors[0], CompileError::UnresolvedRef { kind, name }
            if kind == "Identity" && name == "nonexistent")
        );
    }

    #[test]
    fn validate_identity_refs_deduplicates_across_sync_stages() {
        let mut resources = empty_categorized();
        let mut spec = SyncSpec::new(SyncStep::command(vec!["true".to_string()]));
        spec.identity = Some("missing".to_string());
        spec.run.identity = Some("missing".to_string());
        resources.syncs.insert("s".to_string(), spec);
        let errors = validate_identity_refs(&resources);
        // Both references produce separate errors (same name, but each ref is validated independently).
        assert_eq!(errors.len(), 2);
    }
}
