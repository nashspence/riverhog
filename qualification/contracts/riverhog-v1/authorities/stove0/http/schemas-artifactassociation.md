# schemas: ArtifactAssociation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactassociation:bcd46a5164 -->

Associate classified artifacts without assigning device meaning to Stove0.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-52c5a88ad023"></a>
- <a id="s-9b5d4ccabe5f"></a>`title`: ArtifactAssociation
- <a id="s-1d1d09ce9901"></a>`description`: Associate classified artifacts without assigning device meaning to Stove0.
- <a id="s-53cb845b33c4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bcfb55c83b3c"></a>`associated_roles` | yes | type="array"; minItems=1; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-9bb2b2a360a5"></a>`path_identity` | no | type="string"; const="same-parent-stem" |  |
| <a id="s-dbc2f028571d"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field associated_roles](#s-bcfb55c83b3c) | `cardinality · items · operational_policy` | shared above |

## Governing policies

- <a id="pa-163474883308"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-bf08d4d2202a"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactAssociation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00a5b3ff7e9d421eedaa499625122b72ab475c426ae33f3193ca4bb26500ed7d -->

```json
{
  "additionalProperties": false,
  "description": "Associate classified artifacts without assigning device meaning to Stove0.",
  "properties": {
    "associated_roles": {
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "minItems": 1,
      "title": "Associated Roles",
      "type": "array"
    },
    "path_identity": {
      "const": "same-parent-stem",
      "default": "same-parent-stem",
      "title": "Path Identity",
      "type": "string"
    },
    "primary_role": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Primary Role",
      "type": "string"
    }
  },
  "required": [
    "primary_role",
    "associated_roles"
  ],
  "title": "ArtifactAssociation",
  "type": "object"
}
```
