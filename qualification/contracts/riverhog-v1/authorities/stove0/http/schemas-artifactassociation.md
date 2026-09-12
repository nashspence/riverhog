# schemas: ArtifactAssociation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactassociation:bcd46a5164 -->

Associate classified artifacts without assigning device meaning to Stove0.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: ArtifactAssociation
- `description`: Associate classified artifacts without assigning device meaning to Stove0.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `associated_roles` | yes | type="array"; minItems=1; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| `path_identity` | no | type="string"; const="same-parent-stem" |  |
| `primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

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
