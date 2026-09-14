# lifecycle_events.SQLiteLifecycleEventLog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqlitelifecycleeventlog:c7cc9f77e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a8d194ead"></a>
- <a id="s-79cba93bfc"></a>`distribution`: `lifecycle-events`
- <a id="s-d669083f6b"></a>`module`: `lifecycle_events`
- <a id="s-0735a842cc"></a>`name`: `SQLiteLifecycleEventLog`
- <a id="s-6b835221be"></a>`unit`: `export`

### Declared structure

- <a id="s-f3ac3ef0de"></a>`kind`: `"class"`
- <a id="s-79a5122f02"></a>`signature`: `"\"(connect: 'Callable[[], sqlite3.Connection]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [lifecycle_events.SQLiteLifecycleEventLog.expire_context](lifecycle-events-sqlitelifecycleeventlog-expire-context.md)
- [lifecycle_events.SQLiteLifecycleEventLog.append_once](lifecycle-events-sqlitelifecycleeventlog-append-once.md)
- [lifecycle_events.SQLiteLifecycleEventLog.append](lifecycle-events-sqlitelifecycleeventlog-append.md)
- [lifecycle_events.SQLiteLifecycleEventLog.initialize](lifecycle-events-sqlitelifecycleeventlog-initialize.md)
- [lifecycle_events.SQLiteLifecycleEventLog.page](lifecycle-events-sqlitelifecycleeventlog-page.md)

## Governing policies

- <a id="pa-550c4fee33"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteLifecycleEventLog`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 112e876506955086099d10aa673e0d5dd65d7799a8bcf633a4592cd2924630d4 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(connect: 'Callable[[], sqlite3.Connection]') -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "SQLiteLifecycleEventLog",
  "unit": "export"
}
```
