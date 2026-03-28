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

#[cfg(test)]
mod tests {
    use super::*;

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
}
