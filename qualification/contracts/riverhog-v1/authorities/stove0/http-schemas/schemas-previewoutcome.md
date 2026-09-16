# schemas: PreviewOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-previewoutcome:2081f91de6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-0e24eaf1b1"></a>

- <a id="s-daadc86e25"></a>`type`: `"object"`
- <a id="s-862dd8948e"></a>`additionalProperties`: `false`
- <a id="s-cb14171513"></a>`required`: `["code","message"]`
- <a id="s-d17a225c74"></a>`title`: `"PreviewOutcome"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22b7766e69"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ba9e0e963b"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-9b87a0448c"></a>`retryable` | no | anyOf=(type="boolean") \| (type="null") |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1000; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field message](#s-ba9e0e963b) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-b9a0de2b61"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-501355e02d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/PreviewOutcome`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aadb99fcf4c17ef573a7c408d01e471d92f225af9f5eee9137fb980cbd4f3124 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
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
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "title": "Retryable"
    }
  },
  "required": [
    "code",
    "message"
  ],
  "title": "PreviewOutcome",
  "type": "object"
}
```

</details>
