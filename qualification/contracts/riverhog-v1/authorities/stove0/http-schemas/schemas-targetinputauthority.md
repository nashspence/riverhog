# schemas: TargetInputAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetinputauthority:8aab344b5f -->

Small exact input authority retained by Stove0 and traversed in bounded pages.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c055730868"></a>

- <a id="s-4382919497"></a>`type`: `"object"`
- <a id="s-2bb9698db2"></a>`additionalProperties`: `false`
- <a id="s-984d982d2b"></a>`description`: `"Small exact input authority retained by Stove0 and traversed in bounded pages."`
- <a id="s-a018ba6868"></a>`required`: `["selection","roles"]`
- <a id="s-1c7e42741e"></a>`title`: `"TargetInputAuthority"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdcbb7a5df"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](schemas-targetinputrolecount.md)); minItems=1; title="Roles" |  |
| <a id="s-2b06eb28e8"></a>`selection` | yes | [ArtifactSelectionRef](schemas-artifactselectionref.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field roles](#s-bdcbb7a5df) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArtifactSelectionRef](schemas-artifactselectionref.md)
- [TargetInputRoleCount](schemas-targetinputrolecount.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9f6b58f805"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-1574cc05b8"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetInputAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7657079845a0ca1d03a8be5b49bc8fed556926c933f440039ff04ced820a6598 -->

```json
{
  "additionalProperties": false,
  "description": "Small exact input authority retained by Stove0 and traversed in bounded pages.",
  "properties": {
    "roles": {
      "items": {
        "$ref": "#/components/schemas/TargetInputRoleCount"
      },
      "minItems": 1,
      "title": "Roles",
      "type": "array"
    },
    "selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    }
  },
  "required": [
    "selection",
    "roles"
  ],
  "title": "TargetInputAuthority",
  "type": "object"
}
```

</details>
