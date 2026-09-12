# schemas: EvaluationMatrix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationmatrix:2e11971a77 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-42674da55c"></a>
- <a id="s-4bc22de259"></a>`title`: EvaluationMatrix
- <a id="s-b2fcf12d3e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-36c2f17f34"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1" |  |
| <a id="s-184e486a2e"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4177bbefab"></a>`variants` | yes | type="array"; minItems=1; items=(#/components/schemas/EvaluationVariant) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field variants](#s-4177bbefab) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field matrix_sha256](#s-184e486a2e) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: EvaluationVariant](schemas-evaluationvariant.md)

## Governing policies

- <a id="pa-ead8b39ec8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-7facfed9df"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-d2abf9af60"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationMatrix`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2ca6c94739f3ab55f1dd9a80c12df7af452e1bab0cff3d020cdc543918b73e9 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-evaluation-matrix/v1",
      "default": "stove0-evaluation-matrix/v1",
      "title": "Format",
      "type": "string"
    },
    "matrix_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Matrix Sha256",
      "type": "string"
    },
    "variants": {
      "items": {
        "$ref": "#/components/schemas/EvaluationVariant"
      },
      "minItems": 1,
      "title": "Variants",
      "type": "array"
    }
  },
  "required": [
    "variants",
    "matrix_sha256"
  ],
  "title": "EvaluationMatrix",
  "type": "object"
}
```
