# lifecycle_events.EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-eventpage:8ee91e3310 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-97fc9498e1"></a>
| Field | Shape |
|---|---|
| <a id="s-5c94c6f57d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-97815d04e4"></a>`distribution` | "lifecycle-events" |
| <a id="s-88319aa5d7"></a>`module` | "lifecycle_events" |
| <a id="s-7ff3a644d0"></a>`name` | "EventPage" |
| <a id="s-b44473440b"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [lifecycle_events.EventPage.require_progress_after](lifecycle-events-eventpage-require-progress-after.md)

## Governing policies

- <a id="pa-f0a223d5f6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.EventPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa478a43c4c8007ca18cb5f7a9f95352c1f432e2bcf76389afb467dc7b34a2c6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8847c9798649b25d96f5d871b15ead3ffee87f978f685526fc5b91476d6a03e9",
    "signature": "'(*, events: list[lifecycle_events.models.CloudEvent], next_cursor: str, has_more: bool) -> None'"
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "EventPage",
  "unit": "export"
}
```
