# lifecycle_events.LifecycleEventClient.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-lifecycleeventclient-enter:0ad7b5d94e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b59ece2a7"></a>
| Field | Shape |
|---|---|
| <a id="s-11999f4f9f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e2a5e3b054"></a>`distribution` | "lifecycle-events" |
| <a id="s-f86953edf2"></a>`module` | "lifecycle_events" |
| <a id="s-af2d2ae192"></a>`name` | "__enter__" |
| <a id="s-6bb64f50bc"></a>`owner` | "lifecycle_events.LifecycleEventClient" |
| <a id="s-eae6b11783"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [lifecycle_events.LifecycleEventClient](lifecycle-events-lifecycleeventclient.md)

## Governing policies

- <a id="pa-5c7dc61701"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.LifecycleEventClient.__enter__`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f948629a635d5f10c652319a8bd51da71c12387c9d7ce046dad101ea5a0dd1f3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'LifecycleEventClient'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "__enter__",
  "owner": "lifecycle_events.LifecycleEventClient",
  "unit": "member"
}
```
