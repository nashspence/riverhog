# lifecycle_events.SQLiteLifecycleEventLog.append_once

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqlitelifecycleeventlog-append-once:45de4aa4bc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ad7e233c6"></a>
- <a id="s-aff9c2828d"></a>`distribution`: `lifecycle-events`
- <a id="s-a4f4bee032"></a>`module`: `lifecycle_events`
- <a id="s-0009390c3d"></a>`name`: `append_once`
- <a id="s-fc1ca750ed"></a>`owner`: `lifecycle_events.SQLiteLifecycleEventLog`
- <a id="s-343feb8e2a"></a>`unit`: `member`

### Declared structure

- <a id="s-5501702f76"></a>`kind`: `"method"`
- <a id="s-9cc1246c49"></a>`signature`: `"\"(self, event: 'CloudEvent', *, owner: 'str', context: 'dict[str, object] \| None' = None, context_expires_at: 'str \| None' = None) -> 'int'\""`

## Maintained corroboration

### Related interface records

- [SQLiteLifecycleEventLog](lifecycle-events-sqlitelifecycleeventlog.md)

## Governing policies

- <a id="pa-f7579b703f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteLifecycleEventLog.append_once`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 609873e555c740c9eb2bdc708d4912f93e63067003fbbf0e77c9b7a3efb99898 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, event: 'CloudEvent', *, owner: 'str', context: 'dict[str, object] | None' = None, context_expires_at: 'str | None' = None) -> 'int'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "append_once",
  "owner": "lifecycle_events.SQLiteLifecycleEventLog",
  "unit": "member"
}
```

</details>
