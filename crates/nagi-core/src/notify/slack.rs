use serde::Serialize;

use super::{Notifier, NotifyError, NotifyEvent};

pub struct SlackNotifier {
    webhook_url: String,
}

impl SlackNotifier {
    pub fn new(webhook_url: String) -> Self {
        Self { webhook_url }
    }
}

#[derive(Serialize)]
struct SlackMessage {
    text: String,
}

impl Notifier for SlackNotifier {
    async fn notify(&self, event: &NotifyEvent) -> Result<(), NotifyError> {
        let message = SlackMessage {
            text: event.to_string(),
        };
        let resp = reqwest::Client::new()
            .post(&self.webhook_url)
            .json(&message)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(NotifyError::Status(resp.status().as_u16()));
        }
        Ok(())
    }
}
