# lifecycle_events.SQLiteEventCursorStore.advance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqliteeventcursorstore-advance:070fb6d546 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c67b8a129c"></a>
| Field | Shape |
|---|---|
| <a id="s-68b0fa9532"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a461fb4576"></a>`distribution` | "lifecycle-events" |
| <a id="s-a9b4875323"></a>`module` | "lifecycle_events" |
| <a id="s-50bbeab54d"></a>`name` | "advance" |
| <a id="s-33814b8252"></a>`owner` | "lifecycle_events.SQLiteEventCursorStore" |
| <a id="s-e429a97dcb"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [lifecycle_events.SQLiteEventCursorStore](lifecycle-events-sqliteeventcursorstore.md)

## Governing policies

- <a id="pa-2d921a0c5b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteEventCursorStore.advance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67f58064fceddd28b1301803a85b3fd5b01b75a3d723fc1520cd36ce70fa5b33 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source: 'str', cursor: 'str') -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "advance",
  "owner": "lifecycle_events.SQLiteEventCursorStore",
  "unit": "member"
}
```
