# schemas: WorkFailureView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workfailureview:2dbb61c015 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-b4761108c0"></a>
- <a id="s-80e79fb104"></a>`title`: WorkFailureView
- <a id="s-e20222ca3a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7402188896"></a>`code` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-c750f10414"></a>`message` | yes | type="string"; minLength=1; maxLength=1000 |  |
| <a id="s-ee8f4b742c"></a>`retryable` | yes | type="boolean" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field code](#s-7402188896) | `length · characters · contract_max` | maximum=160 |
| [field message](#s-c750f10414) | `length · characters · contract_max` | maximum=1000 |

## Governing policies

- <a id="pa-ba433ae57a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-6026fad1e2"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkFailureView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a61374424ae470a2c694ac13502faf98f91af05992638c02e1d5b5380dac0c4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Code",
      "type": "string"
    },
    "message": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Message",
      "type": "string"
    },
    "retryable": {
      "title": "Retryable",
      "type": "boolean"
    }
  },
  "required": [
    "code",
    "message",
    "retryable"
  ],
  "title": "WorkFailureView",
  "type": "object"
}
```
