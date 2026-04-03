use std::collections::{HashMap, HashSet};

use super::CompileOutput;
use crate::runtime::kind::connection::ResolvedConnection;

/// Detects if a sync step uses a dbt command that updates multiple Assets.
/// Returns a reason string if problematic, `None` otherwise.
pub fn detect_multi_asset_step(args: &[String]) -> Option<String> {
    if args.iter().any(|a| a == "dbt") && args.iter().any(|a| a == "build") {
        return Some(
            "uses `dbt build` which updates multiple models in a single execution".to_string(),
        );
    }
    if let Some(tag) = args.iter().find(|a| a.starts_with("tag:")) {
        return Some(format!(
            "uses tag-based selector '{tag}' which may update multiple models in a single execution",
        ));
    }
    None
}

/// Finds the dbt Cloud credentials file path from resolved assets' connections.
pub fn find_dbt_cloud_credentials(output: &CompileOutput) -> Option<String> {
    output
        .assets
        .iter()
        .find_map(|asset| match &asset.connection {
            Some(ResolvedConnection::Dbt {
                dbt_cloud_credentials_file: Some(path),
                ..
            }) => Some(path.clone()),
            _ => None,
        })
}

/// Sets `dbt_cloud_job_ids` on each asset based on the model-to-job mapping.
pub fn apply_cloud_job_mapping(
    output: &mut CompileOutput,
    model_job_mapping: &HashMap<String, HashSet<i64>>,
) {
    for asset in &mut output.assets {
        if let Some(job_ids) = model_job_mapping.get(&asset.metadata.name) {
            asset.dbt_cloud_job_ids = Some(job_ids.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::compile::{CompileOutput, DependencyGraph, ResolvedAsset};
    use crate::runtime::kind::asset::AssetSpec;
    use crate::runtime::kind::Metadata;

    fn args(strs: &[&str]) -> Vec<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    macro_rules! detect_multi_asset_step_test {
        ($($name:ident: $args:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let a = args($args);
                    let result = detect_multi_asset_step(&a);
                    assert_eq!(result.is_some(), $expected);
                }
            )*
        };
    }

    detect_multi_asset_step_test! {
        detect_dbt_build: &["dbt", "build", "--select", "model_a"] => true;
        detect_dbt_build_no_select: &["dbt", "build"] => true;
        detect_tag_selector: &["dbt", "run", "--select", "tag:finance"] => true;
        detect_tag_selector_combo: &["dbt", "run", "-s", "tag:finance,tag:daily"] => true;
        ignore_model_select: &["dbt", "run", "--select", "my_model"] => false;
        ignore_non_dbt_command: &["python", "run.py"] => false;
        ignore_empty_args: &[] => false;
    }

    // ── helpers ──────────────────────────────────────────────────────────

    fn make_resolved_asset(name: &str) -> ResolvedAsset {
        ResolvedAsset {
            metadata: Metadata {
                name: name.to_string(),
            },
            model_name: name.to_string(),
            spec: AssetSpec {
                tags: vec![],
                upstreams: vec![],
                on_drift: vec![],
                connection: None,
                auto_sync: true,
                evaluate_cache_ttl: None,
                model_name: None,
            },
            resolved_on_drift: vec![],
            connection: None,
            dbt_cloud_job_ids: None,
        }
    }

    fn make_output(assets: Vec<ResolvedAsset>) -> CompileOutput {
        CompileOutput {
            assets,
            graph: DependencyGraph {
                nodes: vec![],
                edges: vec![],
            },
        }
    }

    // ── apply_cloud_job_mapping ──────────────────────────────────────────

    #[test]
    fn apply_cloud_job_mapping_sets_matching_ids() {
        let mut output = make_output(vec![
            make_resolved_asset("daily_sales"),
            make_resolved_asset("customers"),
            make_resolved_asset("orders"),
        ]);
        let mapping = HashMap::from([
            ("daily_sales".to_string(), HashSet::from([1, 2])),
            ("customers".to_string(), HashSet::from([3])),
        ]);

        apply_cloud_job_mapping(&mut output, &mapping);

        assert_eq!(
            output.assets[0].dbt_cloud_job_ids,
            Some(HashSet::from([1, 2]))
        );
        assert_eq!(output.assets[1].dbt_cloud_job_ids, Some(HashSet::from([3])));
        assert_eq!(output.assets[2].dbt_cloud_job_ids, None);
    }

    #[test]
    fn apply_cloud_job_mapping_empty_mapping_leaves_none() {
        let mut output = make_output(vec![make_resolved_asset("daily_sales")]);

        apply_cloud_job_mapping(&mut output, &HashMap::new());

        assert_eq!(output.assets[0].dbt_cloud_job_ids, None);
    }

    // ── find_dbt_cloud_credentials ──────────────────────────────────────

    #[test]
    fn find_credentials_with_dbt_connection() {
        let mut asset = make_resolved_asset("daily_sales");
        asset.connection = Some(ResolvedConnection::Dbt {
            name: "my-bq".to_string(),
            profile: "my_project".to_string(),
            target: None,
            profiles_dir: None,
            dbt_cloud_credentials_file: Some("/home/user/.dbt/dbt_cloud.yml".to_string()),
        });

        let output = make_output(vec![asset]);
        assert_eq!(
            find_dbt_cloud_credentials(&output),
            Some("/home/user/.dbt/dbt_cloud.yml".to_string())
        );
    }

    #[test]
    fn find_credentials_without_credentials_file() {
        let mut asset = make_resolved_asset("daily_sales");
        asset.connection = Some(ResolvedConnection::Dbt {
            name: "my-bq".to_string(),
            profile: "my_project".to_string(),
            target: None,
            profiles_dir: None,
            dbt_cloud_credentials_file: None,
        });

        let output = make_output(vec![asset]);
        assert_eq!(find_dbt_cloud_credentials(&output), None);
    }

    #[test]
    fn find_credentials_no_connection() {
        let output = make_output(vec![make_resolved_asset("daily_sales")]);
        assert_eq!(find_dbt_cloud_credentials(&output), None);
    }

    #[test]
    fn find_credentials_skips_bigquery_connection() {
        let mut asset = make_resolved_asset("daily_sales");
        asset.connection = Some(ResolvedConnection::BigQuery {
            name: "my-bq".to_string(),
            project: "my-project".to_string(),
            dataset: "my_dataset".to_string(),
            execution_project: None,
            method: None,
            keyfile: None,
            timeout_seconds: None,
        });

        let output = make_output(vec![asset]);
        assert_eq!(find_dbt_cloud_credentials(&output), None);
    }

    #[test]
    fn find_credentials_finds_dbt_among_mixed_connections() {
        let mut bq_asset = make_resolved_asset("orders");
        bq_asset.connection = Some(ResolvedConnection::BigQuery {
            name: "my-bq".to_string(),
            project: "my-project".to_string(),
            dataset: "my_dataset".to_string(),
            execution_project: None,
            method: None,
            keyfile: None,
            timeout_seconds: None,
        });

        let mut dbt_asset = make_resolved_asset("daily_sales");
        dbt_asset.connection = Some(ResolvedConnection::Dbt {
            name: "my-dbt".to_string(),
            profile: "my_project".to_string(),
            target: None,
            profiles_dir: None,
            dbt_cloud_credentials_file: Some("/home/user/.dbt/dbt_cloud.yml".to_string()),
        });

        let output = make_output(vec![bq_asset, dbt_asset]);
        assert_eq!(
            find_dbt_cloud_credentials(&output),
            Some("/home/user/.dbt/dbt_cloud.yml".to_string())
        );
    }
}
