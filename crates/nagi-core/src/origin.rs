use crate::compile::CompileError;
use crate::kind::NagiKind;

/// Expands Origin resources by loading external project data and generating Assets/Syncs.
pub fn expand(resources: Vec<NagiKind>) -> Result<Vec<NagiKind>, CompileError> {
    // Currently only DBT Origins exist. When new Origin types are added,
    // dispatch by OriginSpec variant here.
    crate::dbt::origin::expand(resources)
}

/// Detects if a command conflicts with per-Asset reconciliation.
///
/// Each Origin type checks whether the given args match its known
/// multi-asset patterns (e.g. dbt checks for `dbt build` and tag-based selectors).
/// When new Origin types are added, chain their detectors here.
pub fn detect_multi_asset_command(args: &[String]) -> Option<String> {
    crate::dbt::origin::detect_multi_asset_step(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(strs: &[&str]) -> Vec<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn detect_multi_asset_command_returns_none_for_safe_command() {
        assert!(
            detect_multi_asset_command(&args(&["dbt", "run", "--select", "my_model"])).is_none()
        );
    }

    #[test]
    fn detect_multi_asset_command_detects_problematic_command() {
        assert!(detect_multi_asset_command(&args(&["dbt", "build"])).is_some());
    }

    #[test]
    fn detect_multi_asset_command_returns_none_for_empty() {
        assert!(detect_multi_asset_command(&args(&[])).is_none());
    }
}
