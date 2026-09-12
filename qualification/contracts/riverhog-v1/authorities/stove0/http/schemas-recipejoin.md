# schemas: RecipeJoin

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipejoin:36972c33e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-62829ebffc7e"></a>
- <a id="s-006b649cde38"></a>`title`: RecipeJoin
- <a id="s-fca6274d44a9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cabb20a9f525"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c9445dc761af"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| <a id="s-3f09da3f00cc"></a>`intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-ef3d4bd9a9f5"></a>`members` | yes | type="array"; minItems=2; items=(#/components/schemas/RecipeJoinMember) |  |
| <a id="s-3a69110b26fa"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ce2768e698f7"></a>`projections` | no | type="array"; items=(#/components/schemas/OperationProjection) |  |
| <a id="s-0b3cfd21e6c9"></a>`target_options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-6ad86b13a3fe"></a>`target_registration_id` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field intent](#s-3f09da3f00cc) | `cardinality · entries · operational_policy` | shared above |
| [field members](#s-ef3d4bd9a9f5) | `cardinality · items · operational_policy` | shared above |
| [field projections](#s-ce2768e698f7) | `cardinality · items · operational_policy` | shared above |
| [field target_options](#s-0b3cfd21e6c9) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: OperationProjection](schemas-operationprojection.md)
- [schemas: RecipeJoinMember](schemas-recipejoinmember.md)

## Governing policies

- <a id="pa-55de58a7bea9"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-f1d8bcf11e84"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeJoin`

### Exact owned JSON

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
