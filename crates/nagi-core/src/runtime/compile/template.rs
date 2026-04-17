use std::collections::HashMap;

use crate::runtime::kind::asset::DesiredCondition;
use crate::runtime::kind::sync::{SyncSpec, SyncStep};

use super::{dbt, CompileError, ResolvedAsset};

pub(super) fn require_sync_ref(
    syncs: &HashMap<String, SyncSpec>,
    name: &str,
) -> Result<(), CompileError> {
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
pub(super) fn expand_step(
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
        timeout: step.timeout.clone(),
    }
}

pub(super) fn expand_sync_templates(
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
        timeout: sync_spec.timeout.clone(),
    }
}

/// Expands template variables in a DesiredCondition's args.
pub(super) fn expand_condition_templates(
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
            timeout,
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
            timeout: timeout.clone(),
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

pub(super) fn warn_multi_asset_sync(name: &str, spec: &SyncSpec) {
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
pub(super) enum ReadinessWarning {
    NoOnDrift,
    NoEvalTriggers,
}

pub(super) fn check_readiness_warning(asset: &ResolvedAsset) -> Option<ReadinessWarning> {
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

pub(super) fn warn_asset_readiness(assets: &[ResolvedAsset]) {
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

#[cfg(test)]
mod tests {
    use super::*;

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
            timeout: None,
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
            timeout: None,
        };
        let mut with = HashMap::new();
        with.insert("target".into(), "prod".into());
        let result = expand_step(&step, "a", "b", &with);
        assert_eq!(result.args, vec!["prod"]);
    }
}
