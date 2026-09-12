# schemas: TargetProductionAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetproductionauthority:5d406c80aa -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetProductionAuthority`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactDispositionSetIdentity](schemas-artifactdispositionsetidentity.md)
- [schemas: OutputArtifactSetIdentity](schemas-outputartifactsetidentity.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: TargetProductionAuthority
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `disposition_count` | yes | integer |  |
| `disposition_sha256` | yes | string |  |
| `format` | no | string |  |
| `job_id` | yes | string |  |
| `outputs` | yes | #/components/schemas/OutputArtifactSetIdentity |  |
| `plan_sha256` | yes | string |  |
| `production_sha256` | yes | string |  |
| `riverhog_disposition_set` | yes | #/components/schemas/ArtifactDispositionSetIdentity |  |
| `source_edge_count` | yes | integer |  |
| `source_edge_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f52dbbdc45a3bad1264269aeec9fc665a9b79c6a478c0af16d3ccd200f588c8b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "disposition_count": {
      "minimum": 1,
      "title": "Disposition Count",
      "type": "integer"
    },
    "disposition_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Disposition Sha256",
      "type": "string"
    },
    "format": {
      "const": "stove0-target-production/v1",
      "default": "stove0-target-production/v1",
      "title": "Format",
      "type": "string"
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "outputs": {
      "$ref": "#/components/schemas/OutputArtifactSetIdentity"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "production_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Production Sha256",
      "type": "string"
    },
    "riverhog_disposition_set": {
      "$ref": "#/components/schemas/ArtifactDispositionSetIdentity"
    },
    "source_edge_count": {
      "minimum": 1,
      "title": "Source Edge Count",
      "type": "integer"
    },
    "source_edge_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Edge Sha256",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "plan_sha256",
    "outputs",
    "disposition_count",
    "disposition_sha256",
    "source_edge_count",
    "source_edge_sha256",
    "riverhog_disposition_set",
    "production_sha256"
  ],
  "title": "TargetProductionAuthority",
  "type": "object"
}
```
