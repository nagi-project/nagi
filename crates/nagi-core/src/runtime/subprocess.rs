use std::collections::HashMap;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubprocessEnvError {
    #[error("undefined environment variable '{0}' referenced in ${{...}} template")]
    UndefinedVar(String),

    #[error("invalid ${{...}} template: {message}")]
    InvalidTemplate { message: String },

    #[error("invalid environment variable name '{0}': must match [A-Za-z_][A-Za-z0-9_]*")]
    InvalidKeyName(String),
}

#[cfg(unix)]
const ALLOWLIST: &[&str] = &[
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR",
];

#[cfg(windows)]
const ALLOWLIST: &[&str] = &[
    "SystemRoot",
    "SystemDrive",
    "ComSpec",
    "PATH",
    "PATHEXT",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "APPDATA",
    "LOCALAPPDATA",
    "TEMP",
    "TMP",
    "ProgramData",
    "ProgramFiles",
    "ProgramFiles(x86)",
    "CommonProgramFiles",
    "CommonProgramFiles(x86)",
    "COMPUTERNAME",
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
];

/// Validates that an env var name matches POSIX rules `[A-Za-z_][A-Za-z0-9_]*`.
pub fn validate_env_key(key: &str) -> Result<(), SubprocessEnvError> {
    if is_valid_env_name(key) {
        Ok(())
    } else {
        Err(SubprocessEnvError::InvalidKeyName(key.to_string()))
    }
}

/// Validates all env key names in a map, returning a `KindError::InvalidSpec`
/// on the first invalid key.
pub fn validate_env_keys(
    env: &HashMap<String, String>,
    kind: &str,
    context: &str,
) -> Result<(), crate::runtime::kind::KindError> {
    for key in env.keys() {
        validate_env_key(key).map_err(|e| crate::runtime::kind::KindError::InvalidSpec {
            kind: kind.to_string(),
            message: format!("{context}: {e}"),
        })?;
    }
    Ok(())
}

fn is_valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn expand_template(
    value: &str,
    parent_env: &HashMap<String, String>,
) -> Result<String, SubprocessEnvError> {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| SubprocessEnvError::InvalidTemplate {
                message: "unclosed '${' template".to_string(),
            })?;
        let var_name = &after[..end];
        if !is_valid_env_name(var_name) {
            return Err(SubprocessEnvError::InvalidTemplate {
                message: format!("invalid variable name '{var_name}' in ${{...}} template"),
            });
        }
        let resolved = parent_env
            .get(var_name)
            .ok_or_else(|| SubprocessEnvError::UndefinedVar(var_name.to_string()))?;
        out.push_str(resolved);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

pub(crate) fn compose_subprocess_env(
    parent_env: &HashMap<String, String>,
    declared: &HashMap<String, String>,
) -> Result<HashMap<String, String>, SubprocessEnvError> {
    let mut out = HashMap::new();
    for key in ALLOWLIST {
        if let Some(value) = parent_env.get(*key) {
            out.insert((*key).to_string(), value.clone());
        }
    }
    for (key, value) in declared {
        validate_env_key(key)?;
        let expanded = expand_template(value, parent_env)?;
        out.insert(key.clone(), expanded);
    }
    Ok(out)
}

/// Call immediately before spawning a subprocess and drop the returned map as
/// soon as the subprocess has received the values, so that credentials
/// referenced via `${VAR}` are not retained beyond the spawn point.
pub fn build_subprocess_env(
    declared: &HashMap<String, String>,
) -> Result<HashMap<String, String>, SubprocessEnvError> {
    let parent_env: HashMap<String, String> = std::env::vars().collect();
    compose_subprocess_env(&parent_env, declared)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hm(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // ── allow-list selection ─────────────────────────────────────────

    macro_rules! allowlist_test {
        ($($name:ident: parent=$parent:expr, declared=$declared:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let parent = hm(&$parent);
                    let declared = hm(&$declared);
                    let result = compose_subprocess_env(&parent, &declared).unwrap();
                    assert_eq!(result, hm(&$expected));
                }
            )*
        };
    }

    // `PATH` is in the allow-list on both Unix and Windows; `MY_SECRET` is on
    // neither. These tests therefore run cross-platform.
    allowlist_test! {
        allowlisted_parent_passes_through:
            parent=[("PATH", "/usr/bin")], declared=[] => [("PATH", "/usr/bin")];

        non_allowlisted_parent_is_dropped:
            parent=[("MY_SECRET", "hunter2")], declared=[] => [];

        mixed_allowlisted_and_non_allowlisted:
            parent=[("PATH", "/usr/bin"), ("MY_SECRET", "hunter2")], declared=[]
            => [("PATH", "/usr/bin")];

        allowlist_key_absent_in_parent_is_omitted:
            parent=[], declared=[] => [];

        declared_only_no_parent:
            parent=[], declared=[("FOO", "bar")] => [("FOO", "bar")];
    }

    // ── ${VAR} expansion ─────────────────────────────────────────────

    macro_rules! expansion_test {
        ($($name:ident: parent=$parent:expr, value=$value:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let parent = hm(&$parent);
                    let declared = hm(&[("X", $value)]);
                    let result = compose_subprocess_env(&parent, &declared).unwrap();
                    assert_eq!(result.get("X").map(String::as_str), Some($expected));
                }
            )*
        };
    }

    expansion_test! {
        simple_var:
            parent=[("FOO", "bar")], value="${FOO}" => "bar";

        literal_mixed_with_var:
            parent=[("FOO", "bar")], value="prefix-${FOO}-suffix" => "prefix-bar-suffix";

        multiple_vars:
            parent=[("A", "1"), ("B", "2")], value="${A}/${B}" => "1/2";

        literal_only:
            parent=[], value="plain" => "plain";

        dollar_without_brace_is_literal:
            parent=[], value="p@$sword" => "p@$sword";

        double_dollar_then_var:
            parent=[("FOO", "bar")], value="$${FOO}" => "$bar";
    }

    // ── ${VAR} expansion failures ────────────────────────────────────

    #[test]
    fn undefined_var_errors() {
        let parent = hm(&[]);
        let declared = hm(&[("X", "${FOO}")]);
        let err = compose_subprocess_env(&parent, &declared).unwrap_err();
        assert_eq!(err, SubprocessEnvError::UndefinedVar("FOO".to_string()));
    }

    #[test]
    fn unclosed_template_errors() {
        let parent = hm(&[]);
        let declared = hm(&[("X", "${FOO")]);
        let err = compose_subprocess_env(&parent, &declared).unwrap_err();
        assert!(matches!(err, SubprocessEnvError::InvalidTemplate { .. }));
    }

    #[test]
    fn empty_template_errors() {
        let parent = hm(&[]);
        let declared = hm(&[("X", "${}")]);
        let err = compose_subprocess_env(&parent, &declared).unwrap_err();
        assert!(matches!(err, SubprocessEnvError::InvalidTemplate { .. }));
    }

    #[test]
    fn invalid_var_name_in_template_errors() {
        let parent = hm(&[("FOO-BAR", "x")]);
        let declared = hm(&[("X", "${FOO-BAR}")]);
        let err = compose_subprocess_env(&parent, &declared).unwrap_err();
        assert!(matches!(err, SubprocessEnvError::InvalidTemplate { .. }));
    }

    // ── declared precedence ──────────────────────────────────────────

    macro_rules! precedence_test {
        ($($name:ident: parent=$parent:expr, declared=$declared:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let parent = hm(&$parent);
                    let declared = hm(&$declared);
                    let result = compose_subprocess_env(&parent, &declared).unwrap();
                    assert_eq!(result, hm(&$expected));
                }
            )*
        };
    }

    precedence_test! {
        declared_overrides_allowlisted_key:
            parent=[("PATH", "/usr/bin")], declared=[("PATH", "/opt/bin")]
            => [("PATH", "/opt/bin")];

        declared_adds_non_allowlisted_key:
            parent=[], declared=[("MY_VAR", "hello")]
            => [("MY_VAR", "hello")];
    }

    #[test]
    fn declared_references_allowlisted_parent_via_template() {
        // On Unix HOME is allowlisted; on Windows USERPROFILE is. Use the
        // platform's own home-equivalent to keep the test cross-platform.
        #[cfg(unix)]
        let home_key = "HOME";
        #[cfg(windows)]
        let home_key = "USERPROFILE";

        let parent = hm(&[(home_key, "/root")]);
        let declared = hm(&[("DBT_PROFILES_DIR", "${HOME_REF}/.dbt")]);
        // Replace the placeholder with the actual key name per-platform.
        let declared: HashMap<String, String> = declared
            .into_iter()
            .map(|(k, v)| (k, v.replace("HOME_REF", home_key)))
            .collect();

        let result = compose_subprocess_env(&parent, &declared).unwrap();
        assert_eq!(
            result.get("DBT_PROFILES_DIR").map(String::as_str),
            Some("/root/.dbt")
        );
        assert_eq!(result.get(home_key).map(String::as_str), Some("/root"));
    }

    // ── platform allow-list content ──────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn unix_allowlist_contains_path_and_home() {
        assert!(ALLOWLIST.contains(&"PATH"));
        assert!(ALLOWLIST.contains(&"HOME"));
        assert!(ALLOWLIST.contains(&"LANG"));
        assert!(!ALLOWLIST.contains(&"SHELL"));
        assert!(!ALLOWLIST.contains(&"SSH_AUTH_SOCK"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_allowlist_contains_expected_keys() {
        assert!(ALLOWLIST.contains(&"SystemRoot"));
        assert!(ALLOWLIST.contains(&"SystemDrive"));
        assert!(ALLOWLIST.contains(&"PATH"));
        assert!(ALLOWLIST.contains(&"PATHEXT"));
        assert!(ALLOWLIST.contains(&"USERPROFILE"));
        assert!(ALLOWLIST.contains(&"HOMEDRIVE"));
        assert!(ALLOWLIST.contains(&"HOMEPATH"));
        assert!(ALLOWLIST.contains(&"ProgramData"));
        assert!(ALLOWLIST.contains(&"ProgramFiles"));
        assert!(ALLOWLIST.contains(&"COMPUTERNAME"));
    }

    // ── env key name validation ──────────────────────────────────────

    macro_rules! valid_key_test {
        ($($name:ident: $key:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    validate_env_key($key).unwrap();
                }
            )*
        };
    }

    valid_key_test! {
        key_simple: "FOO";
        key_with_underscore_prefix: "_FOO";
        key_with_underscore_mid: "FOO_BAR";
        key_with_digits: "FOO123";
        key_single_letter: "A";
        key_single_underscore: "_";
    }

    macro_rules! invalid_key_test {
        ($($name:ident: $key:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let err = validate_env_key($key).unwrap_err();
                    assert!(matches!(err, SubprocessEnvError::InvalidKeyName(_)));
                }
            )*
        };
    }

    invalid_key_test! {
        key_with_hyphen: "FOO-BAR";
        key_starts_with_digit: "123VAR";
        key_with_space: "FOO BAR";
        key_empty: "";
        key_with_dot: "FOO.BAR";
    }

    // ── public API smoke test ────────────────────────────────────────

    #[test]
    fn public_api_applies_declared() {
        // Exercises the public API path that snapshots the real parent env.
        // The result depends on the host's env, so only declared passthrough
        // is asserted here; parent env filtering is covered exhaustively by
        // the inner function tests above.
        let declared = hm(&[("NAGI_TEST_DECLARED", "value")]);
        let result = build_subprocess_env(&declared).unwrap();
        assert_eq!(
            result.get("NAGI_TEST_DECLARED").map(String::as_str),
            Some("value")
        );
    }

    #[test]
    fn invalid_declared_key_errors_during_build() {
        let parent = hm(&[]);
        let declared = hm(&[("FOO-BAR", "x")]);
        let err = compose_subprocess_env(&parent, &declared).unwrap_err();
        assert_eq!(
            err,
            SubprocessEnvError::InvalidKeyName("FOO-BAR".to_string())
        );
    }
}
