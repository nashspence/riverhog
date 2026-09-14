# lifecycle_events.SQLiteEventCursorStore

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqliteeventcursorstore:659a2c3bea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b23620ae06"></a>
- <a id="s-0b83dcc641"></a>`distribution`: `lifecycle-events`
- <a id="s-ef4666d1fd"></a>`module`: `lifecycle_events`
- <a id="s-46db195a11"></a>`name`: `SQLiteEventCursorStore`
- <a id="s-22497db3e2"></a>`unit`: `export`

### Declared structure

- <a id="s-c2d713c086"></a>`kind`: `"class"`
- <a id="s-30c26ac4c1"></a>`signature`: `"\"(connect: 'Callable[[], sqlite3.Connection]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [advance](lifecycle-events-sqliteeventcursorstore-advance.md)
- [cursor](lifecycle-events-sqliteeventcursorstore-cursor.md)
- [initialize](lifecycle-events-sqliteeventcursorstore-initialize.md)

## Governing policies

- <a id="pa-d612f62810"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteEventCursorStore`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00845e63f20f24528bba993d02dcc1c27c8203dc98f3081b547a51f084f99148 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(connect: 'Callable[[], sqlite3.Connection]') -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "SQLiteEventCursorStore",
  "unit": "export"
}
```
