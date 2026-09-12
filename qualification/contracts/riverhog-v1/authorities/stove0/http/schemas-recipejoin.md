# schemas: RecipeJoin

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipejoin:36972c33e8 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeJoin`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: OperationProjection](schemas-operationprojection.md)
- [schemas: RecipeJoinMember](schemas-recipejoinmember.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: RecipeJoin
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `id` | yes | string |  |
| `input_retrieval_policy` | no | string |  |
| `intent` | no | object |  |
| `members` | yes | array |  |
| `operation_id` | yes | string |  |
| `projections` | no | array |  |
| `target_options` | no | object |  |
| `target_registration_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56e92fb1573aca06ca8b910b729ac841680cfbf5f1e9d77e5ec2f700eeae8869 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "input_retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Input Retrieval Policy",
      "type": "string"
    },
    "intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Intent",
      "type": "object"
    },
    "members": {
      "items": {
        "$ref": "#/components/schemas/RecipeJoinMember"
      },
      "minItems": 2,
      "title": "Members",
      "type": "array"
    },
    "operation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Operation Id",
      "type": "string"
    },
    "projections": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/OperationProjection"
      },
      "title": "Projections",
      "type": "array"
    },
    "target_options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Target Options",
      "type": "object"
    },
    "target_registration_id": {
      "title": "Target Registration Id",
      "type": "string"
    }
  },
  "required": [
    "id",
    "members",
    "operation_id",
    "target_registration_id"
  ],
  "title": "RecipeJoin",
  "type": "object"
}
```
