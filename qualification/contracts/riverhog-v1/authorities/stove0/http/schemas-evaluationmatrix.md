# schemas: EvaluationMatrix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationmatrix:2e11971a77 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: EvaluationMatrix
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `format` | no | type="string"; const="stove0-evaluation-matrix/v1" |  |
| `matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `variants` | yes | type="array"; minItems=1; items=(#/components/schemas/EvaluationVariant) |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: EvaluationVariant](schemas-evaluationvariant.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
