# schemas: TargetInputAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetinputauthority:3498698ec3 -->

Small exact input authority retained by Stove0 and traversed in bounded pages.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c05573086849"></a>
- <a id="s-1c7e42741e57"></a>`title`: TargetInputAuthority
- <a id="s-984d982d2b0e"></a>`description`: Small exact input authority retained by Stove0 and traversed in bounded pages.
- <a id="s-438291949734"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdcbb7a5df22"></a>`roles` | yes | type="array"; minItems=1; items=(#/components/schemas/TargetInputRoleCount) |  |
| <a id="s-2b06eb28e8e6"></a>`selection` | yes | #/components/schemas/ArtifactSelectionRef |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field roles](#s-bdcbb7a5df22) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: TargetInputRoleCount](schemas-targetinputrolecount.md)

## Governing policies

- <a id="pa-cba4aa39936a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-99e432058dfc"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetInputAuthority`

### Exact owned JSON

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
