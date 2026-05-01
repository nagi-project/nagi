# Notifications

Sends notifications for events that occur during [`nagi serve`](../reference/cli.md#serve) execution.

## Events

| Event | Trigger condition |
| --- | --- |
| EvaluateFailed | When Evaluate fails with an error |
| Suspended | When Guardrails stops a Sync |
| SyncLockSkipped | When Sync lock acquisition reaches the retry limit and the Sync is skipped |

!!! tip
    Even if notifications are not configured or notification delivery fails, the serve loop is not affected. If delivery fails, a warning-level log is emitted.

## Slack

Notifications for the same Asset are grouped into a Slack thread. The first notification becomes the parent message, and subsequent notifications are posted to the same thread.

To send notifications to Slack, complete the following setup:

1. Create a Slack App and grant the `chat:write` scope
2. Invite the App to the target channel
3. Set the target channel in `notify.slack.channel` in [nagi.yaml](../reference/project.md)
4. Set the Bot Token in the environment variable `SLACK_BOT_TOKEN`
