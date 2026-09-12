# schemas: OutputArtifactSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-outputartifactsetidentity:554653c0fa -->

Small identity for target outputs already registered with Riverhog.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: OutputArtifactSetIdentity
- `description`: Small identity for target outputs already registered with Riverhog.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_count` | yes | type="integer"; minimum=1 |  |
| `roles` | yes | type="array"; minItems=1; items=(#/components/schemas/OutputArtifactRoleCount) |  |
| `sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `total_bytes` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: OutputArtifactRoleCount](schemas-outputartifactrolecount.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OutputArtifactSetIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b8358c7d637b9865f6fa839ce95492058b0b03b126cf898f334be2dd855020c -->

```json
{
  "additionalProperties": false,
  "description": "Small identity for target outputs already registered with Riverhog.",
  "properties": {
    "artifact_count": {
      "minimum": 1,
      "title": "Artifact Count",
      "type": "integer"
    },
    "roles": {
      "items": {
        "$ref": "#/components/schemas/OutputArtifactRoleCount"
      },
      "minItems": 1,
      "title": "Roles",
      "type": "array"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    },
    "total_bytes": {
      "minimum": 0,
      "title": "Total Bytes",
      "type": "integer"
    }
  },
  "required": [
    "artifact_count",
    "total_bytes",
    "roles",
    "sha256"
  ],
  "title": "OutputArtifactSetIdentity",
  "type": "object"
}
```
