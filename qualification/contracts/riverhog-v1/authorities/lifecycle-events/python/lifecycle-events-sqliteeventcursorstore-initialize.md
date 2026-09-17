# lifecycle_events.SQLiteEventCursorStore.initialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqliteeventcursorstore-initialize:8c7db4baee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-535bdf349a"></a>
- <a id="s-fa99efb5c1"></a>`distribution`: `lifecycle-events`
- <a id="s-54a218fa5b"></a>`module`: `lifecycle_events`
- <a id="s-4015504d46"></a>`name`: `initialize`
- <a id="s-3af1c18e4d"></a>`owner`: `lifecycle_events.SQLiteEventCursorStore`
- <a id="s-1359bde738"></a>`unit`: `member`

### Declared structure

- <a id="s-6a58e54b8b"></a>`kind`: `"method"`
- <a id="s-8fc156b568"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SQLiteEventCursorStore](lifecycle-events-sqliteeventcursorstore.md)

## Governing policies

- <a id="pa-9323c0f647"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteEventCursorStore.initialize`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42659c02634af2f7e76abac431d92372e395a122439290f8a8c0ffe2290713ed -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "initialize",
  "owner": "lifecycle_events.SQLiteEventCursorStore",
  "unit": "member"
}
```

</details>
