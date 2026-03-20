pub mod slack;

use std::fmt;
use std::future::Future;

#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    #[error("http error")]
    Http(#[from] reqwest::Error),
    #[error("unexpected status: {0}")]
    Status(u16),
}

#[derive(Debug, Clone, PartialEq)]
pub enum NotifyEvent {
    Suspended { asset_name: String, reason: String },
    Halted { reason: String },
}

impl fmt::Display for NotifyEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifyEvent::Suspended { asset_name, reason } => {
                write!(f, "[nagi] Asset `{asset_name}` suspended: {reason}")
            }
            NotifyEvent::Halted { reason } => {
                write!(f, "[nagi] All assets halted: {reason}")
            }
        }
    }
}

pub trait Notifier: Send + Sync {
    fn notify(&self, event: &NotifyEvent) -> impl Future<Output = Result<(), NotifyError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

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
