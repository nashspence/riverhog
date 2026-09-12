# schemas: TargetProgress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetprogress:06cefed426 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-8286fd2437"></a>
- <a id="s-b3591114ae"></a>`title`: TargetProgress
- <a id="s-34d45a8485"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-76d78c2b9f"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-ded255a1ab"></a>`phase` | yes | type="string"; minLength=1; maxLength=120 |  |
| <a id="s-edfb3f4d44"></a>`total` | no | anyOf=type="integer"; minimum=0 \| type="null" |  |
| <a id="s-5abc9e6e1c"></a>`unit` | no | anyOf=type="string"; minLength=1; maxLength=40 \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field phase](#s-ded255a1ab) | `length · characters · contract_max` | maximum=120 |
| <a id="s-df67201d67"></a>[field unit · string value](#s-5abc9e6e1c) | `length · characters · contract_max` | maximum=40 |

## Governing policies

- <a id="pa-48c4325efb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-8a728fab5c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetProgress`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39df6fbc583523c116848af049b9e80abb672fa8af47887fcd5f7a791684498a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "completed": {
      "minimum": 0,
      "title": "Completed",
      "type": "integer"
    },
    "phase": {
      "maxLength": 120,
      "minLength": 1,
      "title": "Phase",
      "type": "string"
    },
    "total": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Total"
    },
    "unit": {
      "anyOf": [
        {
          "maxLength": 40,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Unit"
    }
  },
  "required": [
    "phase",
    "completed"
  ],
  "title": "TargetProgress",
  "type": "object"
}
```
