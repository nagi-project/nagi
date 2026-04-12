use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::KindError;

pub const KIND: &str = "Identity";

/// Spec for `kind: Identity`. Declares an authentication scope that can be referenced
/// from Connection / Sync / Command conditions to control which credentials are exposed
/// to in-process DB clients and subprocess execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum IdentitySpec {
    /// Declares environment variables to inject into the target scope. Values may contain
    /// `${VAR}` references that are expanded from the Nagi process environment at use time.
    Env { env: HashMap<String, String> },
}

impl IdentitySpec {
    pub fn validate(&self) -> Result<(), KindError> {
        match self {
            IdentitySpec::Env { env } => {
                if env.is_empty() {
                    return Err(KindError::InvalidSpec {
                        kind: KIND.to_string(),
                        message: "spec.env must not be empty".to_string(),
                    });
                }
                if env.keys().any(|k| k.is_empty()) {
                    return Err(KindError::InvalidSpec {
                        kind: KIND.to_string(),
                        message: "spec.env keys must not be empty".to_string(),
                    });
                }
                Ok(())
            }
        }
    }
}

/// Runtime error raised when an Identity's env template cannot be expanded.
#[allow(dead_code)] // wired up in the subprocess-env step
#[derive(Debug, Error, PartialEq)]
pub enum IdentityError {
    #[error(
        "identity '{identity}': environment variable '{var}' referenced by key '{key}' is not set"
    )]
    UndefinedVar {
        identity: String,
        key: String,
        var: String,
    },
    #[error("identity '{identity}': value for key '{key}' contains unterminated '${{' reference")]
    UnterminatedReference { identity: String, key: String },
}

/// Resolved reference to a `kind: Identity` resource. Carries the unexpanded env template
/// so that `${VAR}` references stay as-is until `expand_env` is called immediately before
/// the point of use. Credentials produced by expansion never live on this struct.
#[allow(dead_code)] // wired up in the subprocess-env step
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedIdentity {
    pub name: String,
    template: IdentityTemplate,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
enum IdentityTemplate {
    Env { env: HashMap<String, String> },
}

#[allow(dead_code)]
impl ResolvedIdentity {
    pub fn from_spec(name: impl Into<String>, spec: &IdentitySpec) -> Self {
        let template = match spec {
            IdentitySpec::Env { env } => IdentityTemplate::Env { env: env.clone() },
        };
        Self {
            name: name.into(),
            template,
        }
    }

    /// Expand `${VAR}` references using the supplied lookup. The returned map is the
    /// caller's responsibility: use it immediately and drop at the end of the enclosing
    /// scope. Expanded values are never stored on `self`.
    pub fn expand_env<F>(&self, lookup: F) -> Result<HashMap<String, String>, IdentityError>
    where
        F: Fn(&str) -> Option<String>,
    {
        match &self.template {
            IdentityTemplate::Env { env } => {
                let mut out = HashMap::with_capacity(env.len());
                for (key, value) in env {
                    let expanded = expand_value(&self.name, key, value, &lookup)?;
                    out.insert(key.clone(), expanded);
                }
                Ok(out)
            }
        }
    }
}

#[allow(dead_code)]
fn expand_value<F>(
    identity: &str,
    key: &str,
    value: &str,
    lookup: &F,
) -> Result<String, IdentityError>
where
    F: Fn(&str) -> Option<String>,
{
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next();
            let mut var = String::new();
            let mut closed = false;
            for vc in chars.by_ref() {
                if vc == '}' {
                    closed = true;
                    break;
                }
                var.push(vc);
            }
            if !closed {
                return Err(IdentityError::UnterminatedReference {
                    identity: identity.to_string(),
                    key: key.to_string(),
                });
            }
            let replacement = lookup(&var).ok_or_else(|| IdentityError::UndefinedVar {
                identity: identity.to_string(),
                key: key.to_string(),
                var: var.clone(),
            })?;
            out.push_str(&replacement);
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_identity_env_spec_with_literal_value() {
        let yaml = r#"
type: env
env:
  GOOGLE_APPLICATION_CREDENTIALS: /path/to/key.json
"#;
        let spec: IdentitySpec = serde_yaml::from_str(yaml).unwrap();
        let IdentitySpec::Env { env } = spec;
        assert_eq!(
            env.get("GOOGLE_APPLICATION_CREDENTIALS"),
            Some(&"/path/to/key.json".to_string())
        );
    }

    #[test]
    fn parse_identity_env_spec_preserves_var_reference() {
        let yaml = r#"
type: env
env:
  GOOGLE_APPLICATION_CREDENTIALS: ${NAGI_EVAL_KEYFILE}
"#;
        let spec: IdentitySpec = serde_yaml::from_str(yaml).unwrap();
        let IdentitySpec::Env { env } = spec;
        assert_eq!(
            env.get("GOOGLE_APPLICATION_CREDENTIALS"),
            Some(&"${NAGI_EVAL_KEYFILE}".to_string())
        );
    }

    #[test]
    fn parse_identity_env_spec_missing_env_field_fails() {
        let yaml = r#"
type: env
"#;
        let result: Result<IdentitySpec, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn validate_rejects_empty_env_map() {
        let spec = IdentitySpec::Env {
            env: HashMap::new(),
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { .. }));
    }

    #[test]
    fn validate_rejects_empty_env_key() {
        let mut env = HashMap::new();
        env.insert(String::new(), "value".to_string());
        let spec = IdentitySpec::Env { env };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { .. }));
    }

    fn resolved_with(value: &str) -> ResolvedIdentity {
        let mut env = HashMap::new();
        env.insert("KEY".to_string(), value.to_string());
        let spec = IdentitySpec::Env { env };
        ResolvedIdentity::from_spec("test-id", &spec)
    }

    fn fake_lookup(var: &str) -> Option<String> {
        match var {
            "VAR" => Some("value".to_string()),
            "NAGI_EVAL_KEYFILE" => Some("/secret/path".to_string()),
            _ => None,
        }
    }

    macro_rules! expand_env_success_test {
        ($($name:ident: $input:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let resolved = resolved_with($input);
                    let result = resolved.expand_env(fake_lookup).unwrap();
                    assert_eq!(result.get("KEY"), Some(&$expected.to_string()));
                }
            )*
        };
    }

    expand_env_success_test! {
        expand_literal_value_unchanged: "literal" => "literal";
        expand_full_var_reference: "${NAGI_EVAL_KEYFILE}" => "/secret/path";
        expand_partial_substitution: "prefix-${VAR}-suffix" => "prefix-value-suffix";
        expand_multiple_references: "${VAR}:${NAGI_EVAL_KEYFILE}" => "value:/secret/path";
        expand_adjacent_literal_and_var: "$${VAR}" => "$value";
    }

    #[test]
    fn expand_env_undefined_var_errors() {
        let resolved = resolved_with("${MISSING}");
        let err = resolved.expand_env(fake_lookup).unwrap_err();
        assert!(matches!(
            err,
            IdentityError::UndefinedVar { ref var, .. } if var == "MISSING"
        ));
    }

    #[test]
    fn expand_env_unterminated_reference_errors() {
        let resolved = resolved_with("${UNCLOSED");
        let err = resolved.expand_env(fake_lookup).unwrap_err();
        assert!(matches!(err, IdentityError::UnterminatedReference { .. }));
    }

    #[test]
    fn expand_env_preserves_all_keys() {
        let mut env = HashMap::new();
        env.insert("A".to_string(), "literal".to_string());
        env.insert("B".to_string(), "${VAR}".to_string());
        let spec = IdentitySpec::Env { env };
        let resolved = ResolvedIdentity::from_spec("multi", &spec);
        let result = resolved.expand_env(fake_lookup).unwrap();
        assert_eq!(result.get("A"), Some(&"literal".to_string()));
        assert_eq!(result.get("B"), Some(&"value".to_string()));
    }
}
