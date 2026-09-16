# lifecycle_events.MAX_EVENT_CONTEXT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-max-event-context-bytes:530ef7444a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9036768b6"></a>
- <a id="s-ac638445bb"></a>`distribution`: `lifecycle-events`
- <a id="s-fd4e77a02a"></a>`module`: `lifecycle_events`
- <a id="s-174ae88ec4"></a>`name`: `MAX_EVENT_CONTEXT_BYTES`
- <a id="s-86d9eb46ce"></a>`unit`: `export`

### Declared structure

- <a id="s-242d732991"></a>`kind`: `"constant"`
- <a id="s-cf3918786d"></a>`value`: `4096`

## Governing policies

- <a id="pa-4c8d76b1bc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.MAX_EVENT_CONTEXT_BYTES`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c8b293b6f1d29e46d982ebbbff576948cde974f31030bff23bdafe404e9b3b0f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 4096
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "MAX_EVENT_CONTEXT_BYTES",
  "unit": "export"
}
```

</details>
