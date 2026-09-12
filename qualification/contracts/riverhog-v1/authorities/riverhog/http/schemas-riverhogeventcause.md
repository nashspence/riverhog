# schemas: RiverhogEventCause

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-riverhogeventcause:59b3f4a517 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-cac8d15cc3"></a>
- <a id="s-d64723c060"></a>`title`: RiverhogEventCause
- <a id="s-5c5e1a73be"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a8d0df20f2"></a>`id` | yes | type="string"; minLength=1; maxLength=300 |  |
| <a id="s-55e18d79c3"></a>`source` | yes | type="string"; minLength=1; maxLength=1000 |  |
| <a id="s-abdf2f2efc"></a>`subject` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| <a id="s-a3f28fa3f0"></a>`type` | yes | type="string"; minLength=1; maxLength=300 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field id](#s-a8d0df20f2) | `length · characters · contract_max` | maximum=300 |
| [field source](#s-55e18d79c3) | `length · characters · contract_max` | maximum=1000 |
| <a id="s-153b6d5a3d"></a>[field subject · string value](#s-abdf2f2efc) | `length · characters · contract_max` | maximum=1000 |
| [field type](#s-a3f28fa3f0) | `length · characters · contract_max` | maximum=300 |

## Governing policies

- <a id="pa-11997efc0d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-045d2c6c80"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogEventCause`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac29075e27ddb589a896c56213e369861c512dd5f6d747bf10c49fe01431fcb2 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "source": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Source",
      "type": "string"
    },
    "subject": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "type": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type"
  ],
  "title": "RiverhogEventCause",
  "type": "object"
}
```
