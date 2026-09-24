# schemas: EvaluationBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationbinding:fa0168e99b -->

Immutable membership of one work item in a trial/evaluation matrix.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3fc3b22e69"></a>

- <a id="s-e432e8d081"></a>`type`: `"object"`
- <a id="s-16a889eaf6"></a>`additionalProperties`: `false`
- <a id="s-2ac46e2bde"></a>`description`: `"Immutable membership of one work item in a trial/evaluation matrix."`
- <a id="s-b75d292d1b"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`
- <a id="s-be4827d504"></a>`title`: `"EvaluationBinding"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-308184d938"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Evaluation Id" |  |
| <a id="s-865de73e81"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Matrix Sha256" |  |
| <a id="s-610a9c9368"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Parameters" |  |
| <a id="s-f164968bc6"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Variant Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field parameters](#s-610a9c9368) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field evaluation_id](#s-308184d938) | `length · characters · fixed` | shared above |
| [field matrix_sha256](#s-865de73e81) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [JsonValue](schemas-jsonvalue.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-331bba4b23"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-f79245f6b0"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-42a4a787e0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7e2445a55677db47a8bf6815b4e0bdea90186cd0fa199e2761b789fabc3c17be -->

```json
{
  "additionalProperties": false,
  "description": "Immutable membership of one work item in a trial/evaluation matrix.",
  "properties": {
    "evaluation_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Evaluation Id",
      "type": "string"
    },
    "matrix_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Matrix Sha256",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Parameters",
      "type": "object"
    },
    "variant_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Variant Id",
      "type": "string"
    }
  },
  "required": [
    "evaluation_id",
    "matrix_sha256",
    "variant_id"
  ],
  "title": "EvaluationBinding",
  "type": "object"
}
```

</details>
