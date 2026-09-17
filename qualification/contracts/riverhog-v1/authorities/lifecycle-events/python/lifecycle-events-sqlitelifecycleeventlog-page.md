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
- <a id="s-2aaddbce76"></a>`distribution`: `lifecycle-events`
- <a id="s-0a581b6c41"></a>`module`: `lifecycle_events`
- <a id="s-78432f2e37"></a>`name`: `page`
- <a id="s-c7bd81cccb"></a>`owner`: `lifecycle_events.SQLiteLifecycleEventLog`
- <a id="s-066a7c15ae"></a>`unit`: `member`

### Declared structure

- <a id="s-c183755f25"></a>`kind`: `"method"`
- <a id="s-39c2f081b9"></a>`signature`: `"\"(self, *, after: 'str \| None', limit: 'int', owner: 'str \| None' = None) -> 'EventPage'\""`

## Maintained corroboration

### Related interface records

- [SQLiteLifecycleEventLog](lifecycle-events-sqlitelifecycleeventlog.md)

## Governing policies

- <a id="pa-800ad27d47"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteLifecycleEventLog.page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
