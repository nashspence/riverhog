# schemas: ArtifactDispositionSetIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionsetidentitydocument:e0162f2abb -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionSetIdentityDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ArtifactDispositionSetIdentityDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `disposition_count` | yes | integer |  |
| `output_artifact_count` | yes | integer |  |
| `output_edge_count` | yes | integer |  |
| `sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e58430ac8dbd95e932c4d52b253787a3a26e728ac17e0a7c2d640ae94904af8f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "disposition_count": {
      "minimum": 1,
      "title": "Disposition Count",
      "type": "integer"
    },
    "output_artifact_count": {
      "minimum": 1,
      "title": "Output Artifact Count",
      "type": "integer"
    },
    "output_edge_count": {
      "minimum": 1,
      "title": "Output Edge Count",
      "type": "integer"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "disposition_count",
    "output_edge_count",
    "output_artifact_count",
    "sha256"
  ],
  "title": "ArtifactDispositionSetIdentityDocument",
  "type": "object"
}
```
