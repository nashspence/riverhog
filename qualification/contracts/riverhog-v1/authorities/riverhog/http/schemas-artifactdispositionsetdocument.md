# schemas: ArtifactDispositionSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionsetdocument:208cd19fd9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: ArtifactDispositionSetDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `disposition_count` | yes | type="integer"; minimum=0 |  |
| `failure` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| `identity` | no | anyOf=#/components/schemas/ArtifactDispositionSetIdentityDocument \| type="null" |  |
| `output_artifact_count` | yes | type="integer"; minimum=0 |  |
| `output_edge_count` | yes | type="integer"; minimum=0 |  |
| `state` | yes | type="string"; enum=["receiving","sealing","sealed","failed"] |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionSetDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 193ec959876d910dbd237e2dd02b3d7d30c9441335c81112f9d9631a1dc43523 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "disposition_count": {
      "minimum": 0,
      "title": "Disposition Count",
      "type": "integer"
    },
    "failure": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "identity": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArtifactDispositionSetIdentityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "output_artifact_count": {
      "minimum": 0,
      "title": "Output Artifact Count",
      "type": "integer"
    },
    "output_edge_count": {
      "minimum": 0,
      "title": "Output Edge Count",
      "type": "integer"
    },
    "state": {
      "enum": [
        "receiving",
        "sealing",
        "sealed",
        "failed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "claim_id",
    "state",
    "disposition_count",
    "output_edge_count",
    "output_artifact_count"
  ],
  "title": "ArtifactDispositionSetDocument",
  "type": "object"
}
```
