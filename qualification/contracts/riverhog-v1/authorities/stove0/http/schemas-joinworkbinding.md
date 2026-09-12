# schemas: JoinWorkBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joinworkbinding:7eb3505b26 -->

Stable branch-set lineage for one ordinary join work identity.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-57421a0aa8"></a>
- <a id="s-d74b458e12"></a>`title`: JoinWorkBinding
- <a id="s-c94f020779"></a>`description`: Stable branch-set lineage for one ordinary join work identity.
- <a id="s-a60ed1074f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4034723c28"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0b3cd7bd11"></a>`kind` | no | type="string"; const="join" |  |
| <a id="s-469029f453"></a>`members` | yes | type="array"; minItems=2; items=(#/components/schemas/JoinWorkMemberBinding) |  |
| <a id="s-fbd3cc4a8a"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field members](#s-469029f453) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-4034723c28) | `length · characters · fixed` | shared above |
| [field parent_work_id](#s-fbd3cc4a8a) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JoinWorkMemberBinding](schemas-joinworkmemberbinding.md)

## Governing policies

- <a id="pa-755da28331"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-fca85dc62c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-c304213c70"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinWorkBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f2994411487c2f5a213ce07a72a8092d94a0e6a56dcccb7110296f6f222b472 -->

```json
{
  "additionalProperties": false,
  "description": "Stable branch-set lineage for one ordinary join work identity.",
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "kind": {
      "const": "join",
      "default": "join",
      "title": "Kind",
      "type": "string"
    },
    "members": {
      "items": {
        "$ref": "#/components/schemas/JoinWorkMemberBinding"
      },
      "minItems": 2,
      "title": "Members",
      "type": "array"
    },
    "parent_work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Parent Work Id",
      "type": "string"
    }
  },
  "required": [
    "parent_work_id",
    "branch_set_sha256",
    "members"
  ],
  "title": "JoinWorkBinding",
  "type": "object"
}
```
