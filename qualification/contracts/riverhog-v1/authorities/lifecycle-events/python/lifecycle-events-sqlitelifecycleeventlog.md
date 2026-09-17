# lifecycle_events.SQLiteLifecycleEventLog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqlitelifecycleeventlog:c7cc9f77e8 -->

Exact externally visible contract owned by this contract element.

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

- [expire_context](lifecycle-events-sqlitelifecycleeventlog-expire-context.md)
- [append_once](lifecycle-events-sqlitelifecycleeventlog-append-once.md)
- [append](lifecycle-events-sqlitelifecycleeventlog-append.md)
- [initialize](lifecycle-events-sqlitelifecycleeventlog-initialize.md)
- [page](lifecycle-events-sqlitelifecycleeventlog-page.md)

## Governing policies

- <a id="pa-550c4fee33"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteLifecycleEventLog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
