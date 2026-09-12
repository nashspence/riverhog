# schemas: ArtifactSelection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactselection:0876c0940c -->

One exact, content-addressed selection of immutable artifacts.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: ArtifactSelection
- `description`: One exact, content-addressed selection of immutable artifacts.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_count` | yes | type="integer"; minimum=1 |  |
| `artifacts` | yes | type="array"; minItems=1; items=(#/components/schemas/ArtifactSubject) |  |
| `format` | no | type="string"; const="stove0-artifact-selection/v1" |  |
| `selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `total_bytes` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSubject](schemas-artifactsubject.md)

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

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactSelection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8af48804a1c059e6b44aa3f4ab91efe8fccf666f2e8c8f0bc454359fb12b95ca -->

```json
{
  "additionalProperties": false,
  "description": "One exact, content-addressed selection of immutable artifacts.",
  "properties": {
    "artifact_count": {
      "minimum": 1,
      "title": "Artifact Count",
      "type": "integer"
    },
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/ArtifactSubject"
      },
      "minItems": 1,
      "title": "Artifacts",
      "type": "array"
    },
    "format": {
      "const": "stove0-artifact-selection/v1",
      "default": "stove0-artifact-selection/v1",
      "title": "Format",
      "type": "string"
    },
    "selection_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Selection Sha256",
      "type": "string"
    },
    "total_bytes": {
      "minimum": 0,
      "title": "Total Bytes",
      "type": "integer"
    }
  },
  "required": [
    "artifacts",
    "artifact_count",
    "total_bytes",
    "selection_sha256"
  ],
  "title": "ArtifactSelection",
  "type": "object"
}
```
