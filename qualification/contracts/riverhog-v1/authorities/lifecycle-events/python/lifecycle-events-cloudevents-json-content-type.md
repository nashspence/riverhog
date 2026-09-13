# lifecycle_events.CLOUDEVENTS_JSON_CONTENT_TYPE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-cloudevents-json-content-type:fb8e351cc5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-408787ee18"></a>
| Field | Shape |
|---|---|
| <a id="s-b7fab9eea6"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-80dac40847"></a>`distribution` | "lifecycle-events" |
| <a id="s-238c795757"></a>`module` | "lifecycle_events" |
| <a id="s-1e3dc91d5f"></a>`name` | "CLOUDEVENTS_JSON_CONTENT_TYPE" |
| <a id="s-36d056f44b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c041d36a71"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.CLOUDEVENTS_JSON_CONTENT_TYPE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf4df3e811af84149a9e862237822d353d68c9e5979d1e8cb24a1bd9cf751592 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "application/cloudevents+json"
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "CLOUDEVENTS_JSON_CONTENT_TYPE",
  "unit": "export"
}
```
