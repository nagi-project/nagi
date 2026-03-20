use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::{Notifier, NotifyError, NotifyEvent};

const SLACK_API_URL: &str = "https://slack.com/api/chat.postMessage";
const TOKEN_ENV_VAR: &str = "SLACK_BOT_TOKEN";

pub struct SlackNotifier {
    channel: String,
    /// Per-asset thread timestamps. The first message for an asset becomes the
    /// parent; subsequent messages are posted as replies in the same thread.
    thread_ts: Mutex<HashMap<String, String>>,
}

impl SlackNotifier {
    pub fn new(channel: String) -> Self {
        Self {
            channel,
            thread_ts: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the existing thread_ts for the event's asset, if any.
    fn get_thread_ts(&self, event: &NotifyEvent) -> Option<String> {
        let name = event.asset_name()?;
        self.thread_ts.lock().ok()?.get(name).cloned()
    }

    /// Stores the thread_ts for the event's asset (first message only).
    fn store_thread_ts(&self, event: &NotifyEvent, ts: String) {
        if let Some(asset_name) = event.asset_name() {
            if let Ok(mut map) = self.thread_ts.lock() {
                map.entry(asset_name.to_string()).or_insert(ts);
            }
        }
    }
}

#[derive(Serialize)]
struct SlackPostMessage {
    channel: String,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_ts: Option<String>,
}

#[derive(Deserialize)]
struct SlackResponse {
    ok: bool,
    #[serde(default)]
    ts: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[async_trait::async_trait]
impl Notifier for SlackNotifier {
    async fn notify(&self, event: &NotifyEvent) -> Result<(), NotifyError> {
        let token = std::env::var(TOKEN_ENV_VAR).map_err(|_| NotifyError::MissingToken)?;

        let body = SlackPostMessage {
            channel: self.channel.clone(),
            text: event.to_string(),
            thread_ts: self.get_thread_ts(event),
        };

        let resp = reqwest::Client::new()
            .post(SLACK_API_URL)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(NotifyError::Status(resp.status().as_u16()));
        }

        let slack_resp: SlackResponse = resp.json().await?;
        if !slack_resp.ok {
            let msg = slack_resp.error.unwrap_or_else(|| "unknown".to_string());
            return Err(NotifyError::Api(msg));
        }

        if let Some(ts) = slack_resp.ts {
            self.store_thread_ts(event, ts);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval_failed_event(name: &str) -> NotifyEvent {
        NotifyEvent::EvalFailed {
            asset_name: name.to_string(),
            error: "test".to_string(),
        }
    }

    fn suspended_event(name: &str) -> NotifyEvent {
        NotifyEvent::Suspended {
            asset_name: name.to_string(),
            reason: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn notify_returns_missing_token_when_env_unset() {
        std::env::remove_var(TOKEN_ENV_VAR);

        let notifier = SlackNotifier::new("#test".to_string());
        let err = notifier.notify(&suspended_event("a")).await.unwrap_err();
        assert!(matches!(err, NotifyError::MissingToken));
    }

    #[test]
    fn get_thread_ts_returns_none_for_new_asset() {
        let notifier = SlackNotifier::new("#test".to_string());
        assert!(notifier.get_thread_ts(&eval_failed_event("a")).is_none());
    }

    #[test]
    fn store_and_get_thread_ts() {
        let notifier = SlackNotifier::new("#test".to_string());
        let event = eval_failed_event("a");

        notifier.store_thread_ts(&event, "1234.5678".to_string());
        assert_eq!(
            notifier.get_thread_ts(&event),
            Some("1234.5678".to_string())
        );
    }

    #[test]
    fn store_thread_ts_keeps_first_value() {
        let notifier = SlackNotifier::new("#test".to_string());
        let event = eval_failed_event("a");

        notifier.store_thread_ts(&event, "first".to_string());
        notifier.store_thread_ts(&event, "second".to_string());
        assert_eq!(notifier.get_thread_ts(&event), Some("first".to_string()));
    }

    #[test]
    fn thread_ts_is_per_asset() {
        let notifier = SlackNotifier::new("#test".to_string());

        notifier.store_thread_ts(&eval_failed_event("a"), "ts-a".to_string());
        notifier.store_thread_ts(&suspended_event("b"), "ts-b".to_string());

        assert_eq!(
            notifier.get_thread_ts(&eval_failed_event("a")),
            Some("ts-a".to_string())
        );
        assert_eq!(
            notifier.get_thread_ts(&suspended_event("b")),
            Some("ts-b".to_string())
        );
    }

    #[test]
    fn thread_ts_shared_across_event_types_for_same_asset() {
        let notifier = SlackNotifier::new("#test".to_string());

        notifier.store_thread_ts(&eval_failed_event("a"), "ts-a".to_string());
        // Suspended event for same asset should reuse the thread.
        assert_eq!(
            notifier.get_thread_ts(&suspended_event("a")),
            Some("ts-a".to_string())
        );
    }

    #[test]
    fn halted_event_has_no_thread_ts() {
        let notifier = SlackNotifier::new("#test".to_string());
        let event = NotifyEvent::Halted {
            reason: "test".to_string(),
        };
        notifier.store_thread_ts(&event, "should-not-store".to_string());
        assert!(notifier.get_thread_ts(&event).is_none());
    }
}
