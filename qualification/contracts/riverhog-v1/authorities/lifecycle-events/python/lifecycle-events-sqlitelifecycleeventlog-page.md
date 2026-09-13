# lifecycle_events.SQLiteLifecycleEventLog.page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqlitelifecycleeventlog-page:50e21b6a4c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7112d1666f"></a>
| Field | Shape |
|---|---|
| <a id="s-15853eaca6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2aaddbce76"></a>`distribution` | "lifecycle-events" |
| <a id="s-0a581b6c41"></a>`module` | "lifecycle_events" |
| <a id="s-78432f2e37"></a>`name` | "page" |
| <a id="s-c7bd81cccb"></a>`owner` | "lifecycle_events.SQLiteLifecycleEventLog" |
| <a id="s-066a7c15ae"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [lifecycle_events.SQLiteLifecycleEventLog](lifecycle-events-sqlitelifecycleeventlog.md)

## Governing policies

- <a id="pa-800ad27d47"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteLifecycleEventLog.page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b10473a644e32310da10290c64ae62730dfc11384a58ad89be05ab7614e9a85f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, after: 'str | None', limit: 'int', owner: 'str | None' = None) -> 'EventPage'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "page",
  "owner": "lifecycle_events.SQLiteLifecycleEventLog",
  "unit": "member"
}
```
