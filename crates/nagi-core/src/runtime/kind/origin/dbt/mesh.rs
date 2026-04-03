use std::collections::HashMap;

use crate::runtime::compile::CompileError;
use crate::runtime::kind::origin::dbt::manifest::{DbtManifest, DbtNode, DbtSource};
use crate::runtime::kind::NagiKind;

/// dbt Relation (database, schema, identifier) for matching
/// a source to a model's output. All components are lowercased
/// for case-insensitive matching.
///
/// `origin` and `asset_name` record which Origin produced this Relation.
/// These are set when building from a model node, and left empty when
/// building from a source node (used only as a lookup key).
#[derive(Debug, Clone)]
struct Relation {
    database: String,
    schema: String,
    identifier: String,
    origin: String,
    asset_name: String,
}

impl PartialEq for Relation {
    fn eq(&self, other: &Self) -> bool {
        self.database == other.database
            && self.schema == other.schema
            && self.identifier == other.identifier
    }
}

impl Eq for Relation {}

impl std::hash::Hash for Relation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.database.hash(state);
        self.schema.hash(state);
        self.identifier.hash(state);
    }
}

impl Relation {
    fn new(database: &str, schema: &str, identifier: &str) -> Self {
        Self {
            database: database.to_lowercase(),
            schema: schema.to_lowercase(),
            identifier: identifier.to_lowercase(),
            origin: String::new(),
            asset_name: String::new(),
        }
    }

    /// Extracts a Relation from a model node's output (database, schema, alias or name).
    fn from_model(node: &DbtNode, origin: &str) -> Option<Self> {
        let database = node.database.as_deref()?;
        let schema = node.schema.as_deref()?;
        let identifier = node.alias.as_deref().unwrap_or(&node.name);
        let mut rel = Self::new(database, schema, identifier);
        rel.origin = origin.to_string();
        rel.asset_name = format!("{origin}.{}", node.name);
        Some(rel)
    }

    /// Extracts a Relation from a source node's reference (database, schema, identifier or name).
    fn from_source(source: &DbtSource) -> Option<Self> {
        let database = source.database.as_deref()?;
        let schema = source.schema.as_deref()?;
        let identifier = source.identifier.as_deref().unwrap_or(&source.name);
        Some(Self::new(database, schema, identifier))
    }
}

// ── Public API ──────────────────────────────────────────────────────────

/// Links cross-Origin sources to their upstream models.
///
/// For each source that points to the same Relation as a model in another
/// Origin, removes the source Asset and updates downstream upstreams to
/// reference the model Asset directly.
pub fn link_sources_to_models(
    resources: Vec<NagiKind>,
    manifests: &HashMap<String, DbtManifest>,
) -> Result<Vec<NagiKind>, CompileError> {
    let model_relations = build_model_relations(manifests);
    check_duplicate_outputs(&model_relations)?;
    let source_to_model = find_linked_sources(manifests, &model_relations);

    if source_to_model.is_empty() {
        return Ok(resources);
    }

    Ok(apply_links(resources, &source_to_model))
}

// ── Build model Relations ────────────────────────────────────────────────

/// Collects model Relations from all Origins.
/// Returns a map from Relation (by database.schema.identifier) to a list of
/// Relations that output to that location (including origin and asset_name).
fn build_model_relations(
    manifests: &HashMap<String, DbtManifest>,
) -> HashMap<Relation, Vec<Relation>> {
    manifests
        .iter()
        .flat_map(|(origin_name, manifest)| collect_model_relations(origin_name, manifest))
        .fold(HashMap::new(), |mut map, relation| {
            map.entry(relation.clone()).or_default().push(relation);
            map
        })
}

/// Extracts Relations from a single manifest's model nodes.
fn collect_model_relations(origin_name: &str, manifest: &DbtManifest) -> Vec<Relation> {
    manifest
        .nodes
        .values()
        .filter(|node| node.resource_type == "model")
        .filter_map(|node| Relation::from_model(node, origin_name))
        .collect()
}

/// Returns an error if multiple Origins output to the same Relation.
fn check_duplicate_outputs(
    model_relations: &HashMap<Relation, Vec<Relation>>,
) -> Result<(), CompileError> {
    model_relations
        .iter()
        .find(|(_, relations)| relations.len() > 1)
        .map(|(key, relations)| {
            let origins: Vec<&str> = relations.iter().map(|r| r.origin.as_str()).collect();
            CompileError::OriginFailed(format!(
                "Relation {}.{}.{} is output by models in multiple Origins: {}",
                key.database,
                key.schema,
                key.identifier,
                origins.join(", ")
            ))
        })
        .map_or(Ok(()), Err)
}

/// For each source that matches a model in a different Origin, returns
/// source_asset_name → model_asset_name.
fn find_linked_sources(
    manifests: &HashMap<String, DbtManifest>,
    model_relations: &HashMap<Relation, Vec<Relation>>,
) -> HashMap<String, String> {
    manifests
        .iter()
        .flat_map(|(origin_name, manifest)| {
            manifest.sources.values().filter_map(move |source| {
                let relation = Relation::from_source(source)?;
                let models = model_relations.get(&relation)?;
                let model = &models[0];
                if model.origin == *origin_name {
                    return None;
                }
                let source_asset_name =
                    format!("{origin_name}.{}.{}", source.source_name, source.name);
                Some((source_asset_name, model.asset_name.clone()))
            })
        })
        .collect()
}

/// Removes linked source Assets and replaces their references in upstreams.
fn apply_links(
    resources: Vec<NagiKind>,
    source_to_model: &HashMap<String, String>,
) -> Vec<NagiKind> {
    resources
        .into_iter()
        .filter_map(|r| match r {
            NagiKind::Asset { metadata, .. } if source_to_model.contains_key(&metadata.name) => {
                None
            }
            NagiKind::Asset {
                api_version,
                metadata,
                mut spec,
            } => {
                spec.upstreams = spec
                    .upstreams
                    .into_iter()
                    .map(|u| source_to_model.get(&u).cloned().unwrap_or(u))
                    .collect();
                Some(NagiKind::Asset {
                    api_version,
                    metadata,
                    spec,
                })
            }
            other => Some(other),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::kind::origin::dbt::manifest::{
        DbtDependsOn, DbtManifest, DbtNode, DbtSource,
    };

    fn make_model(
        unique_id: &str,
        name: &str,
        database: &str,
        schema: &str,
        alias: Option<&str>,
    ) -> DbtNode {
        DbtNode {
            unique_id: unique_id.to_string(),
            resource_type: "model".to_string(),
            name: name.to_string(),
            _package_name: "pkg".to_string(),
            tags: vec![],
            depends_on: DbtDependsOn::default(),
            test_metadata: None,
            database: Some(database.to_string()),
            schema: Some(schema.to_string()),
            alias: alias.map(|s| s.to_string()),
        }
    }

    fn make_source(
        unique_id: &str,
        name: &str,
        source_name: &str,
        database: &str,
        schema: &str,
        identifier: Option<&str>,
    ) -> DbtSource {
        DbtSource {
            unique_id: unique_id.to_string(),
            name: name.to_string(),
            source_name: source_name.to_string(),
            tags: vec![],
            database: Some(database.to_string()),
            schema: Some(schema.to_string()),
            identifier: identifier.map(|s| s.to_string()),
        }
    }

    fn single_model_manifest(
        origin: &str,
        model_id: &str,
        name: &str,
        db: &str,
        schema: &str,
        alias: Option<&str>,
    ) -> (String, DbtManifest) {
        (
            origin.to_string(),
            DbtManifest {
                nodes: HashMap::from([(
                    model_id.to_string(),
                    make_model(model_id, name, db, schema, alias),
                )]),
                sources: HashMap::new(),
            },
        )
    }

    fn single_source_manifest(
        origin: &str,
        source_id: &str,
        name: &str,
        source_name: &str,
        db: &str,
        schema: &str,
        identifier: Option<&str>,
    ) -> (String, DbtManifest) {
        (
            origin.to_string(),
            DbtManifest {
                nodes: HashMap::new(),
                sources: HashMap::from([(
                    source_id.to_string(),
                    make_source(source_id, name, source_name, db, schema, identifier),
                )]),
            },
        )
    }

    // ── Relation::from_model / from_source ─────────────────────────────

    #[test]
    fn relation_from_model_uses_alias() {
        let node = make_model("id", "orders", "mydb", "public", Some("order_tbl"));
        let rel = Relation::from_model(&node, "test").unwrap();
        assert_eq!(rel.identifier, "order_tbl");
    }

    #[test]
    fn relation_from_model_falls_back_to_name() {
        let node = make_model("id", "orders", "mydb", "public", None);
        let rel = Relation::from_model(&node, "test").unwrap();
        assert_eq!(rel.identifier, "orders");
    }

    #[test]
    fn relation_from_model_none_when_no_database() {
        let mut node = make_model("id", "orders", "mydb", "public", None);
        node.database = None;
        assert!(Relation::from_model(&node, "test").is_none());
    }

    #[test]
    fn relation_from_source_uses_identifier() {
        let source = make_source("id", "orders", "raw", "mydb", "public", Some("order_tbl"));
        let rel = Relation::from_source(&source).unwrap();
        assert_eq!(rel.identifier, "order_tbl");
    }

    #[test]
    fn relation_from_source_falls_back_to_name() {
        let source = make_source("id", "orders", "raw", "mydb", "public", None);
        let rel = Relation::from_source(&source).unwrap();
        assert_eq!(rel.identifier, "orders");
    }

    #[test]
    fn relation_from_source_none_when_no_schema() {
        let mut source = make_source("id", "orders", "raw", "mydb", "public", None);
        source.schema = None;
        assert!(Relation::from_source(&source).is_none());
    }

    #[test]
    fn relation_lowercases_components() {
        let node = make_model("id", "Orders", "MYDB", "Public", None);
        let rel = Relation::from_model(&node, "test").unwrap();
        assert_eq!(rel.database, "mydb");
        assert_eq!(rel.schema, "public");
        assert_eq!(rel.identifier, "orders");
    }

    // ── collect_model_relations ─────────────────────────────────────────

    #[test]
    fn collect_model_relations_returns_relations() {
        let (_, manifest) = single_model_manifest(
            "core",
            "model.core.orders",
            "orders",
            "mydb",
            "public",
            None,
        );
        let relations = collect_model_relations("core", &manifest);
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].asset_name, "core.orders");
        assert_eq!(relations[0].origin, "core");
    }

    #[test]
    fn collect_model_relations_skips_tests() {
        let manifest = DbtManifest {
            nodes: HashMap::from([(
                "test.core.t1".to_string(),
                DbtNode {
                    unique_id: "test.core.t1".to_string(),
                    resource_type: "test".to_string(),
                    name: "t1".to_string(),
                    _package_name: "pkg".to_string(),
                    tags: vec![],
                    depends_on: DbtDependsOn::default(),
                    test_metadata: None,
                    database: Some("mydb".to_string()),
                    schema: Some("public".to_string()),
                    alias: None,
                },
            )]),
            sources: HashMap::new(),
        };
        let relations = collect_model_relations("core", &manifest);
        assert!(relations.is_empty());
    }

    #[test]
    fn collect_model_relations_skips_missing_database() {
        let manifest = DbtManifest {
            nodes: HashMap::from([(
                "model.core.orders".to_string(),
                DbtNode {
                    unique_id: "model.core.orders".to_string(),
                    resource_type: "model".to_string(),
                    name: "orders".to_string(),
                    _package_name: "pkg".to_string(),
                    tags: vec![],
                    depends_on: DbtDependsOn::default(),
                    test_metadata: None,
                    database: None,
                    schema: Some("public".to_string()),
                    alias: None,
                },
            )]),
            sources: HashMap::new(),
        };
        let relations = collect_model_relations("core", &manifest);
        assert!(relations.is_empty());
    }

    // ── build_model_relations ───────────────────────────────────────────

    #[test]
    fn build_model_relations_collects_models() {
        let manifests = HashMap::from([single_model_manifest(
            "core",
            "model.core.orders",
            "orders",
            "mydb",
            "public",
            None,
        )]);
        let relations = build_model_relations(&manifests);
        let key = Relation::new("mydb", "public", "orders");
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[&key][0].asset_name, "core.orders");
    }

    #[test]
    fn build_model_relations_uses_alias() {
        let manifests = HashMap::from([single_model_manifest(
            "core",
            "model.core.orders",
            "orders",
            "mydb",
            "public",
            Some("order_table"),
        )]);
        let relations = build_model_relations(&manifests);
        let key = Relation::new("mydb", "public", "order_table");
        assert!(relations.contains_key(&key));
        let key_by_name = Relation::new("mydb", "public", "orders");
        assert!(!relations.contains_key(&key_by_name));
    }

    #[test]
    fn build_model_relations_skips_non_models() {
        let manifests = HashMap::from([(
            "core".to_string(),
            DbtManifest {
                nodes: HashMap::from([(
                    "test.core.t1".to_string(),
                    DbtNode {
                        unique_id: "test.core.t1".to_string(),
                        resource_type: "test".to_string(),
                        name: "t1".to_string(),
                        _package_name: "pkg".to_string(),
                        tags: vec![],
                        depends_on: DbtDependsOn::default(),
                        test_metadata: None,
                        database: Some("mydb".to_string()),
                        schema: Some("public".to_string()),
                        alias: None,
                    },
                )]),
                sources: HashMap::new(),
            },
        )]);
        let relations = build_model_relations(&manifests);
        assert!(relations.is_empty());
    }

    #[test]
    fn build_model_relations_skips_missing_database() {
        let manifests = HashMap::from([(
            "core".to_string(),
            DbtManifest {
                nodes: HashMap::from([(
                    "model.core.orders".to_string(),
                    DbtNode {
                        unique_id: "model.core.orders".to_string(),
                        resource_type: "model".to_string(),
                        name: "orders".to_string(),
                        _package_name: "pkg".to_string(),
                        tags: vec![],
                        depends_on: DbtDependsOn::default(),
                        test_metadata: None,
                        database: None,
                        schema: Some("public".to_string()),
                        alias: None,
                    },
                )]),
                sources: HashMap::new(),
            },
        )]);
        let relations = build_model_relations(&manifests);
        assert!(relations.is_empty());
    }

    // ── check_duplicate_outputs ─────────────────────────────────────────

    #[test]
    fn check_duplicate_outputs_ok_when_unique() {
        let manifests = HashMap::from([
            single_model_manifest(
                "core",
                "model.core.orders",
                "orders",
                "mydb",
                "public",
                None,
            ),
            single_model_manifest(
                "finance",
                "model.finance.invoices",
                "invoices",
                "mydb",
                "public",
                None,
            ),
        ]);
        let relations = build_model_relations(&manifests);
        assert!(check_duplicate_outputs(&relations).is_ok());
    }

    #[test]
    fn check_duplicate_outputs_errors_on_conflict() {
        let manifests = HashMap::from([
            single_model_manifest(
                "core",
                "model.core.orders",
                "orders",
                "mydb",
                "public",
                None,
            ),
            single_model_manifest(
                "finance",
                "model.finance.orders",
                "orders",
                "mydb",
                "public",
                None,
            ),
        ]);
        let relations = build_model_relations(&manifests);
        let err = check_duplicate_outputs(&relations).unwrap_err();
        assert!(err.to_string().contains("multiple Origins"));
    }

    // ── find_linked_sources ───────────────────────────────────────────────────

    #[test]
    fn find_linked_sources_links_cross_origin() {
        let manifests = HashMap::from([
            single_model_manifest(
                "core",
                "model.core.orders",
                "orders",
                "mydb",
                "public",
                None,
            ),
            single_source_manifest(
                "finance",
                "source.finance.raw.orders",
                "orders",
                "raw",
                "mydb",
                "public",
                None,
            ),
        ]);
        let relations = build_model_relations(&manifests);
        let matched = find_linked_sources(&manifests, &relations);
        assert_eq!(
            matched.get("finance.raw.orders"),
            Some(&"core.orders".to_string())
        );
    }

    #[test]
    fn find_linked_sources_ignores_same_origin() {
        let manifests = HashMap::from([(
            "core".to_string(),
            DbtManifest {
                nodes: HashMap::from([(
                    "model.core.orders".to_string(),
                    make_model("model.core.orders", "orders", "mydb", "public", None),
                )]),
                sources: HashMap::from([(
                    "source.core.raw.orders".to_string(),
                    make_source(
                        "source.core.raw.orders",
                        "orders",
                        "raw",
                        "mydb",
                        "public",
                        None,
                    ),
                )]),
            },
        )]);
        let relations = build_model_relations(&manifests);
        let matched = find_linked_sources(&manifests, &relations);
        assert!(matched.is_empty());
    }

    #[test]
    fn find_linked_sources_case_insensitive() {
        let manifests = HashMap::from([
            single_model_manifest(
                "core",
                "model.core.orders",
                "orders",
                "MYDB",
                "PUBLIC",
                None,
            ),
            single_source_manifest(
                "finance",
                "source.finance.raw.orders",
                "orders",
                "raw",
                "mydb",
                "public",
                None,
            ),
        ]);
        let relations = build_model_relations(&manifests);
        let matched = find_linked_sources(&manifests, &relations);
        assert_eq!(matched.len(), 1);
    }

    #[test]
    fn find_linked_sources_no_match() {
        let manifests = HashMap::from([
            single_model_manifest(
                "core",
                "model.core.orders",
                "orders",
                "mydb",
                "public",
                None,
            ),
            single_source_manifest(
                "finance",
                "source.finance.ext.payments",
                "payments",
                "ext",
                "otherdb",
                "public",
                None,
            ),
        ]);
        let relations = build_model_relations(&manifests);
        let matched = find_linked_sources(&manifests, &relations);
        assert!(matched.is_empty());
    }

    #[test]
    fn find_linked_sources_with_alias_and_identifier() {
        let manifests = HashMap::from([
            single_model_manifest(
                "core",
                "model.core.orders",
                "orders",
                "mydb",
                "public",
                Some("order_table"),
            ),
            single_source_manifest(
                "finance",
                "source.finance.raw.orders",
                "orders",
                "raw",
                "mydb",
                "public",
                Some("order_table"),
            ),
        ]);
        let relations = build_model_relations(&manifests);
        let matched = find_linked_sources(&manifests, &relations);
        assert_eq!(matched.len(), 1);
    }

    // ── link_sources_to_models (integration) ────────────────────────────────

    #[test]
    fn link_sources_to_models_suppresses_source_and_rewires() {
        let manifests = HashMap::from([
            single_model_manifest(
                "core",
                "model.core.orders",
                "orders",
                "mydb",
                "public",
                None,
            ),
            single_source_manifest(
                "finance",
                "source.finance.raw.orders",
                "orders",
                "raw",
                "mydb",
                "public",
                None,
            ),
        ]);
        let resources = vec![
            make_asset("core.orders"),
            make_asset_with_upstreams("finance.stg_orders", &["finance.raw.orders"]),
            make_asset("finance.raw.orders"),
        ];
        let result = link_sources_to_models(resources, &manifests).unwrap();
        let names: Vec<&str> = result.iter().map(|r| r.metadata().name.as_str()).collect();
        // finance.raw.orders is suppressed
        assert!(!names.contains(&"finance.raw.orders"));
        // finance.stg_orders upstream is rewired to core.orders
        if let NagiKind::Asset { spec, .. } = result
            .iter()
            .find(|r| r.metadata().name == "finance.stg_orders")
            .unwrap()
        {
            assert_eq!(spec.upstreams, vec!["core.orders"]);
        }
    }

    #[test]
    fn link_sources_to_models_errors_on_duplicate_outputs() {
        let manifests = HashMap::from([
            single_model_manifest(
                "core",
                "model.core.orders",
                "orders",
                "mydb",
                "public",
                None,
            ),
            single_model_manifest(
                "finance",
                "model.finance.orders",
                "orders",
                "mydb",
                "public",
                None,
            ),
        ]);
        let err = link_sources_to_models(vec![], &manifests).unwrap_err();
        assert!(err.to_string().contains("multiple Origins"));
    }

    #[test]
    fn link_sources_to_models_noop_when_no_cross_origin_match() {
        let manifests = HashMap::from([single_model_manifest(
            "core",
            "model.core.orders",
            "orders",
            "mydb",
            "public",
            None,
        )]);
        let resources = vec![make_asset("core.orders")];
        let result = link_sources_to_models(resources, &manifests).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metadata().name, "core.orders");
    }

    // ── test helpers ─────────────────────────────────────────────────────

    fn make_asset(name: &str) -> NagiKind {
        NagiKind::Asset {
            api_version: "nagi.io/v1alpha1".to_string(),
            metadata: crate::runtime::kind::Metadata {
                name: name.to_string(),
            },
            spec: crate::runtime::kind::asset::AssetSpec {
                tags: vec![],
                connection: None,
                upstreams: vec![],
                on_drift: vec![],
                auto_sync: true,
                evaluate_cache_ttl: None,
                model_name: None,
            },
        }
    }

    fn make_sync(name: &str) -> NagiKind {
        NagiKind::Sync {
            api_version: "nagi.io/v1alpha1".to_string(),
            metadata: crate::runtime::kind::Metadata {
                name: name.to_string(),
            },
            spec: crate::runtime::kind::sync::SyncSpec {
                pre: None,
                run: crate::runtime::kind::sync::SyncStep {
                    step_type: crate::runtime::kind::sync::StepType::Command,
                    args: vec![],
                    env: HashMap::new(),
                },
                post: None,
            },
        }
    }

    // ── apply_links ──────────────────────────────────────────────────────

    fn make_asset_with_upstreams(name: &str, upstreams: &[&str]) -> NagiKind {
        NagiKind::Asset {
            api_version: "nagi.io/v1alpha1".to_string(),
            metadata: crate::runtime::kind::Metadata {
                name: name.to_string(),
            },
            spec: crate::runtime::kind::asset::AssetSpec {
                tags: vec![],
                connection: None,
                upstreams: upstreams.iter().map(|s| s.to_string()).collect(),
                on_drift: vec![],
                auto_sync: true,
                evaluate_cache_ttl: None,
                model_name: None,
            },
        }
    }

    #[test]
    fn apply_links_removes_source_and_replaces_upstreams() {
        let source_to_model =
            HashMap::from([("finance.raw.orders".to_string(), "core.orders".to_string())]);
        let resources = vec![
            make_asset("core.orders"),
            make_asset_with_upstreams("finance.stg_orders", &["finance.raw.orders"]),
            make_asset("finance.raw.orders"),
            make_sync("my-sync"),
        ];
        let result = apply_links(resources, &source_to_model);
        let names: Vec<&str> = result.iter().map(|r| r.metadata().name.as_str()).collect();
        assert_eq!(names, vec!["core.orders", "finance.stg_orders", "my-sync"]);
        if let NagiKind::Asset { spec, .. } = &result[1] {
            assert_eq!(spec.upstreams, vec!["core.orders"]);
        }
    }

    #[test]
    fn apply_links_leaves_unmatched_upstreams() {
        let source_to_model =
            HashMap::from([("finance.raw.orders".to_string(), "core.orders".to_string())]);
        let resources = vec![make_asset_with_upstreams(
            "finance.revenue",
            &["finance.raw.payments"],
        )];
        let result = apply_links(resources, &source_to_model);
        if let NagiKind::Asset { spec, .. } = &result[0] {
            assert_eq!(spec.upstreams, vec!["finance.raw.payments"]);
        }
    }

    #[test]
    fn apply_links_preserves_non_assets() {
        let source_to_model =
            HashMap::from([("finance.raw.orders".to_string(), "core.orders".to_string())]);
        let resources = vec![make_sync("finance.raw.orders")];
        let result = apply_links(resources, &source_to_model);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metadata().name, "finance.raw.orders");
    }

    #[test]
    fn apply_links_noop_when_empty_map() {
        let source_to_model = HashMap::new();
        let resources = vec![make_asset("a"), make_asset("b")];
        let result = apply_links(resources, &source_to_model);
        assert_eq!(result.len(), 2);
    }
}
