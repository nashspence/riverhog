# schemas: TargetInputAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetinputauthority:3498698ec3 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetInputAuthority`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: TargetInputRoleCount](schemas-targetinputrolecount.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: TargetInputAuthority
- `description`: Small exact input authority retained by Stove0 and traversed in bounded pages.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `roles` | yes | array |  |
| `selection` | yes | #/components/schemas/ArtifactSelectionRef |  |

## Complete owned contract

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
