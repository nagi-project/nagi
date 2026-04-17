use std::collections::{HashMap, HashSet};

use crate::runtime::kind::asset::{merge_on_drift_entries, AssetSpec, DesiredCondition};
use crate::runtime::kind::connection::ConnectionSpec;
use crate::runtime::kind::sync::SyncSpec;
use crate::runtime::kind::{self, Metadata, NagiKind};

use super::{into_result, CompileError};

#[derive(Debug)]
pub(super) struct CategorizedResources {
    pub connections: HashMap<String, ConnectionSpec>,
    pub conditions_groups: HashMap<String, Vec<DesiredCondition>>,
    pub syncs: HashMap<String, SyncSpec>,
    pub assets: Vec<(Metadata, AssetSpec)>,
    pub identities: HashMap<String, kind::identity::IdentitySpec>,
}

fn check_dup(
    seen: &mut HashSet<(String, String)>,
    kind: String,
    name: String,
) -> Result<(), CompileError> {
    if !seen.insert((kind.clone(), name.clone())) {
        return Err(CompileError::DuplicateName { kind, name });
    }
    Ok(())
}

/// Returns `true` if the resource is new, `false` if duplicate (error pushed to `errors`).
fn check_dup_collect(
    seen: &mut HashSet<(String, String)>,
    errors: &mut Vec<CompileError>,
    kind: String,
    name: String,
) -> bool {
    match check_dup(seen, kind, name) {
        Ok(()) => true,
        Err(e) => {
            errors.push(e);
            false
        }
    }
}

pub(super) fn categorize(resources: Vec<NagiKind>) -> Result<CategorizedResources, CompileError> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut errors: Vec<CompileError> = Vec::new();
    let mut result = CategorizedResources {
        connections: HashMap::new(),
        conditions_groups: HashMap::new(),
        syncs: HashMap::new(),
        assets: Vec::new(),
        identities: HashMap::new(),
    };
    // Track Asset names separately for overlay merge (max 2 allowed).
    let mut asset_indices: HashMap<String, usize> = HashMap::new();

    for resource in resources {
        let kind = resource.kind().to_string();
        let name = resource.metadata().name.clone();
        match resource {
            NagiKind::Asset { metadata, spec, .. } => {
                if let Some(&idx) = asset_indices.get(&name) {
                    if check_dup_collect(&mut seen, &mut errors, kind, name) {
                        let overlay = std::mem::take(&mut result.assets[idx].1.on_drift);
                        result.assets[idx].1.on_drift =
                            merge_on_drift_entries(overlay, spec.on_drift);
                    }
                } else {
                    asset_indices.insert(name, result.assets.len());
                    result.assets.push((metadata, spec));
                }
            }
            NagiKind::Connection { spec, .. } => {
                if check_dup_collect(&mut seen, &mut errors, kind, name.clone()) {
                    result.connections.insert(name, spec);
                }
            }
            NagiKind::Conditions { spec, .. } => {
                if check_dup_collect(&mut seen, &mut errors, kind, name.clone()) {
                    result.conditions_groups.insert(name, spec.0.clone());
                }
            }
            NagiKind::Sync { spec, .. } => {
                if check_dup_collect(&mut seen, &mut errors, kind, name.clone()) {
                    result.syncs.insert(name, *spec);
                }
            }
            NagiKind::Origin { .. } => {}
            NagiKind::Identity { spec, .. } => {
                if check_dup_collect(&mut seen, &mut errors, kind, name.clone()) {
                    result.identities.insert(name, spec);
                }
            }
        }
    }

    into_result(errors)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::kind::parse_kinds;
    use std::collections::HashSet;

    fn parse(yaml: &str) -> Vec<NagiKind> {
        parse_kinds(yaml).unwrap()
    }

    // ── check_dup_collect ────────────────────────────────────────────────

    #[test]
    fn check_dup_collect_returns_true_for_new_entry() {
        let mut seen = HashSet::new();
        let mut errors = Vec::new();
        assert!(check_dup_collect(
            &mut seen,
            &mut errors,
            "Asset".into(),
            "foo".into()
        ));
        assert!(errors.is_empty());
    }

    #[test]
    fn check_dup_collect_different_kinds_same_name() {
        let mut seen = HashSet::new();
        let mut errors = Vec::new();
        check_dup_collect(&mut seen, &mut errors, "Asset".into(), "foo".into());
        assert!(check_dup_collect(
            &mut seen,
            &mut errors,
            "Sync".into(),
            "foo".into()
        ));
        assert!(errors.is_empty());
    }

    #[test]
    fn check_dup_collect_returns_false_and_pushes_error_on_duplicate() {
        let mut seen = HashSet::new();
        let mut errors = Vec::new();
        check_dup_collect(&mut seen, &mut errors, "Asset".into(), "foo".into());
        assert!(!check_dup_collect(
            &mut seen,
            &mut errors,
            "Asset".into(),
            "foo".into()
        ));
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            CompileError::DuplicateName { kind, name } if kind == "Asset" && name == "foo"
        ));
    }

    #[test]
    fn categorize_accumulates_multiple_duplicates() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: conn-a
spec:
  type: dbt
  profile: proj
---
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: conn-a
spec:
  type: dbt
  profile: proj
---
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: sync-a
spec:
  run:
    type: Command
    args: [\"true\"]
---
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: sync-a
spec:
  run:
    type: Command
    args: [\"true\"]",
        );
        let err = categorize(resources).unwrap_err();
        match err {
            CompileError::Multiple(errors) => {
                assert_eq!(errors.len(), 2);
                assert!(errors
                    .iter()
                    .all(|e| matches!(e, CompileError::DuplicateName { .. })));
            }
            other => panic!("expected Multiple, got: {other}"),
        }
    }
}
