# schemas: TargetInputAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetinputauthority:3498698ec3 -->

Small exact input authority retained by Stove0 and traversed in bounded pages.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: TargetInputAuthority
- `description`: Small exact input authority retained by Stove0 and traversed in bounded pages.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `roles` | yes | type="array"; minItems=1; items=(#/components/schemas/TargetInputRoleCount) |  |
| `selection` | yes | #/components/schemas/ArtifactSelectionRef |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: TargetInputRoleCount](schemas-targetinputrolecount.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
