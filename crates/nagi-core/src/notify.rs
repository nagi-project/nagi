pub mod slack;

use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    #[error("http error")]
    Http(#[from] reqwest::Error),
    #[error("unexpected status: {0}")]
    Status(u16),
    #[error("SLACK_BOT_TOKEN environment variable not set")]
    MissingToken,
    #[error("slack api error: {0}")]
    Api(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum NotifyEvent {
    EvalFailed { asset_name: String, error: String },
    Suspended { asset_name: String, reason: String },
    Halted { reason: String },
}

impl NotifyEvent {
    /// Returns the asset name associated with this event, if any.
    /// Used for per-asset thread grouping.
    pub fn asset_name(&self) -> Option<&str> {
        match self {
            NotifyEvent::EvalFailed { asset_name, .. }
            | NotifyEvent::Suspended { asset_name, .. } => Some(asset_name),
            NotifyEvent::Halted { .. } => None,
        }
    }
}

impl fmt::Display for NotifyEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifyEvent::EvalFailed { asset_name, error } => {
                write!(f, "[nagi] Asset `{asset_name}` evaluation failed: {error}")
            }
            NotifyEvent::Suspended { asset_name, reason } => {
                write!(f, "[nagi] Asset `{asset_name}` suspended: {reason}")
            }
            NotifyEvent::Halted { reason } => {
                write!(f, "[nagi] All assets halted: {reason}")
            }
        }
    }
}

#[async_trait::async_trait]
pub trait Notifier: Send + Sync {
    async fn notify(&self, event: &NotifyEvent) -> Result<(), NotifyError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_display_eval_failed() {
        let event = NotifyEvent::EvalFailed {
            asset_name: "daily-sales".to_string(),
            error: "parse error".to_string(),
        };
        assert_eq!(
            event.to_string(),
            "[nagi] Asset `daily-sales` evaluation failed: parse error"
        );
    }

    #[test]
    fn event_display_suspended() {
        let event = NotifyEvent::Suspended {
            asset_name: "daily-sales".to_string(),
            reason: "3 consecutive sync failures".to_string(),
        };
        assert_eq!(
            event.to_string(),
            "[nagi] Asset `daily-sales` suspended: 3 consecutive sync failures"
        );
    }

    #[test]
    fn event_display_halted() {
        let event = NotifyEvent::Halted {
            reason: "manual halt".to_string(),
        };
        assert_eq!(event.to_string(), "[nagi] All assets halted: manual halt");
    }
}
