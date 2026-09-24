# schemas: ArtifactAssociation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-artifactassociation:7b409b55c7 -->

Associate classified artifacts without assigning device meaning to Stove0.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-52c5a88ad0"></a>

- <a id="s-53cb845b33"></a>`type`: `"object"`
- <a id="s-d47e7616af"></a>`additionalProperties`: `false`
- <a id="s-1d1d09ce99"></a>`description`: `"Associate classified artifacts without assigning device meaning to Stove0."`
- <a id="s-3208fb2925"></a>`required`: `["primary_role","associated_roles"]`
- <a id="s-9b5d4ccabe"></a>`title`: `"ArtifactAssociation"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bcfb55c83b"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Associated Roles" |  |
| <a id="s-9bb2b2a360"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem"; title="Path Identity" |  |
| <a id="s-dbc2f02857"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Primary Role" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field associated_roles](#s-bcfb55c83b) | `cardinality · items · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2b03af97e4"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e203d52a70"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactAssociation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
