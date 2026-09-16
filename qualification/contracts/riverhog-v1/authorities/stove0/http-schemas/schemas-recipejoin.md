# schemas: RecipeJoin

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-recipejoin:7a461df3ac -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-62829ebffc"></a>

- <a id="s-fca6274d44"></a>`type`: `"object"`
- <a id="s-9a3f115125"></a>`additionalProperties`: `false`
- <a id="s-7498e71165"></a>`required`: `["id","members","operation_id","target_registration_id"]`
- <a id="s-006b649cde"></a>`title`: `"RecipeJoin"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cabb20a9f5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c9445dc761"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-3f09da3f00"></a>`intent` | no | type="object"; additionalProperties=(#/components/schemas/JsonValue) |  |
| <a id="s-ef3d4bd9a9"></a>`members` | yes | type="array"; items=(#/components/schemas/RecipeJoinMember); minItems=2 |  |
| <a id="s-3a69110b26"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ce2768e698"></a>`projections` | no | type="array"; default=[]; items=(#/components/schemas/OperationProjection) |  |
| <a id="s-0b3cfd21e6"></a>`target_options` | no | type="object"; additionalProperties=(#/components/schemas/JsonValue) |  |
| <a id="s-6ad86b13a3"></a>`target_registration_id` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field intent](#s-3f09da3f00) | `cardinality · entries · operational_policy` | shared above |
| [field members](#s-ef3d4bd9a9) | `cardinality · items · operational_policy` | shared above |
| [field projections](#s-ce2768e698) | `cardinality · items · operational_policy` | shared above |
| [field target_options](#s-0b3cfd21e6) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [JsonValue](schemas-jsonvalue.md)
- [OperationProjection](schemas-operationprojection.md)
- [RecipeJoinMember](schemas-recipejoinmember.md)

## Governing policies

- <a id="pa-5eb5a4dbbd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-5807fc73e4"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeJoin`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
