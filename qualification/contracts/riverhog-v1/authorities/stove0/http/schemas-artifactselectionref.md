# schemas: ArtifactSelectionRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactselectionref:e24d3bebce -->

Closed reference to a separately retained selection document.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: ArtifactSelectionRef
- `description`: Closed reference to a separately retained selection document.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_count` | yes | type="integer"; minimum=1 |  |
| `selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `total_bytes` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactSelectionRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8cd0eea202ee0c8f3d19552216aa1b0d50521d431df3a185e911bfe1c3aa4c7 -->

```json
{
  "additionalProperties": false,
  "description": "Closed reference to a separately retained selection document.",
  "properties": {
    "artifact_count": {
      "minimum": 1,
      "title": "Artifact Count",
      "type": "integer"
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
    "selection_sha256",
    "artifact_count",
    "total_bytes"
  ],
  "title": "ArtifactSelectionRef",
  "type": "object"
}
```
