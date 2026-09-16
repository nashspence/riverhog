# schemas: ProcessingOutcomeBindingDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingoutcomebindingdocument:986f424b61 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-76ce77bed4"></a>

- <a id="s-22171f3d1a"></a>`type`: `"object"`
- <a id="s-b4e6d59e09"></a>`additionalProperties`: `false`
- <a id="s-9b4b9d2ded"></a>`required`: `["claim_id","fence","outcome_id"]`
- <a id="s-443afecedb"></a>`title`: `"ProcessingOutcomeBindingDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c023e38ade"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |  |
| <a id="s-d1a9ffbb93"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-ed65033b3d"></a>`outcome_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Outcome Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-c023e38ade) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-26e114a400"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-913820863c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingOutcomeBindingDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 53b400e47c4282f39ca9413347f8cd65db26d06e70dab9ef036d89feaa731635 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "outcome_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Outcome Id",
      "type": "string"
    }
  },
  "required": [
    "claim_id",
    "fence",
    "outcome_id"
  ],
  "title": "ProcessingOutcomeBindingDocument",
  "type": "object"
}
```

</details>
