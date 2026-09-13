# lifecycle_events.SQLiteEventCursorStore.cursor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqliteeventcursorstore-cursor:3fab398e48 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-29fd6299f8"></a>
| Field | Shape |
|---|---|
| <a id="s-fc52e2c46b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-eb3c82be78"></a>`distribution` | "lifecycle-events" |
| <a id="s-46b21a9d40"></a>`module` | "lifecycle_events" |
| <a id="s-e04e75c419"></a>`name` | "cursor" |
| <a id="s-9899521333"></a>`owner` | "lifecycle_events.SQLiteEventCursorStore" |
| <a id="s-787dcae635"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [lifecycle_events.SQLiteEventCursorStore](lifecycle-events-sqliteeventcursorstore.md)

## Governing policies

- <a id="pa-f6c2e27243"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteEventCursorStore.cursor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35a8514ca98065b2f63aa1c3b4fc9295a7266d163599517800c7c22add057728 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source: 'str') -> 'str'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "cursor",
  "owner": "lifecycle_events.SQLiteEventCursorStore",
  "unit": "member"
}
```
