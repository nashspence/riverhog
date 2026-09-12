# schemas: ArtifactRule

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactrule:1d9ed84d81 -->

Classify one path; first matching rule wins.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ArtifactRule
- `description`: Classify one path; first matching rule wins.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `glob` | no | type="string" |  |
| `media_type` | no | anyOf=type="string" \| type="null" |  |
| `role` | no | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactRule`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3392b5b6ba296932dd8651fd6ad0532db4a7cf5bf186fb0991311dc91d753a1d -->

```json
{
  "additionalProperties": false,
  "description": "Classify one path; first matching rule wins.",
  "properties": {
    "glob": {
      "default": "*",
      "title": "Glob",
      "type": "string"
    },
    "media_type": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Media Type"
    },
    "role": {
      "default": "stove0.source/v1",
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Role",
      "type": "string"
    }
  },
  "title": "ArtifactRule",
  "type": "object"
}
```
