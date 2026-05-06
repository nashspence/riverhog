# ADR-0047: Use Status Notification Payloads

## Decision

Riverhog emits status notifications for long-running operator interactions as a separate payload kind from action-needed notifications.

Status notifications report lifecycle movement only: `started`, `still_running`, `blocked`, `completed`, or `failed`.

Every status payload names the owning operator workflow state with `statechart` and `state`, carries a stable `operation_id`, and uses the status schema in `contracts/operator/status-notification.schema.json`.

Blocked and failed status notifications may reference the related action-needed event, but they do not replace or weaken the action-needed payload contract.

Completed status notifications are routine status records, not routine-success action-needed notifications.

## Reason

Operators and integrations need progress visibility while long-running work is active, but action-needed notifications must stay reserved for decisions or work the operator can do now.
