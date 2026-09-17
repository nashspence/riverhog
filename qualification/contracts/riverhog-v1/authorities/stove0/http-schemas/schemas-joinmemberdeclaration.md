# schemas: JoinMemberDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-joinmemberdeclaration:1e8fe748cb -->

Exact named branch and opaque output roles required by the join.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-1fcec863de"></a>

- <a id="s-0e833bfd3e"></a>`type`: `"object"`
- <a id="s-a4722b4040"></a>`additionalProperties`: `false`
- <a id="s-acd5756001"></a>`description`: `"Exact named branch and opaque output roles required by the join."`
- <a id="s-3323972c1f"></a>`required`: `["branch_id","output_roles"]`
- <a id="s-1bfc0751bb"></a>`title`: `"JoinMemberDeclaration"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-966acc4a4b"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-3a68a48782"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Output Roles" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field output_roles](#s-3a68a48782) | `cardinality · items · operational_policy` | shared above |

## Governing policies

- <a id="pa-de6a38a69f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-0b3bd36e8a"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinMemberDeclaration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f1b1441297c6334a7eeb79e0a05030fe3b536cc2d6ee09a2aa316a623e74c389 -->

```json
{
  "additionalProperties": false,
  "description": "Exact named branch and opaque output roles required by the join.",
  "properties": {
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "output_roles": {
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "minItems": 1,
      "title": "Output Roles",
      "type": "array"
    }
  },
  "required": [
    "branch_id",
    "output_roles"
  ],
  "title": "JoinMemberDeclaration",
  "type": "object"
}
```

</details>
