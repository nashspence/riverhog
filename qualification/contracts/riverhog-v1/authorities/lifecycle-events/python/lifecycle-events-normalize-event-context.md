# lifecycle_events.normalize_event_context

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-normalize-event-context:f3fc413781 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a165d454ee"></a>
| Field | Shape |
|---|---|
| <a id="s-e49f80e872"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-bdbe448537"></a>`distribution` | "lifecycle-events" |
| <a id="s-76a2a71df9"></a>`module` | "lifecycle_events" |
| <a id="s-6f805cd25f"></a>`name` | "normalize_event_context" |
| <a id="s-3c2b8b0712"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3d10725459"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.normalize_event_context`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 102aa87c6c5bfad6f6a09690aa9dce94dec218b921c440840b6e309b77f351dc -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'Mapping[str, Any] | None', *, max_bytes: 'int' = 4096) -> 'dict[str, Any] | None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "normalize_event_context",
  "unit": "export"
}
```
