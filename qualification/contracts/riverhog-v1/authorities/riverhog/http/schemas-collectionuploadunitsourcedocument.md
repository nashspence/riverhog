# schemas: CollectionUploadUnitSourceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitsourcedocument:9513787822 -->

One exact source range supplied in a server-planned upload unit.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: CollectionUploadUnitSourceDocument
- `description`: One exact source range supplied in a server-planned upload unit.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `bytes` | yes | type="integer"; minimum=0 |  |
| `offset` | yes | type="integer"; minimum=0 |  |
| `path` | yes | type="string" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

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
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitSourceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3870870a9e0b96323d9a6bb59e3124806d4f2cbfd6db047e34765004a7c96160 -->

```json
{
  "additionalProperties": false,
  "description": "One exact source range supplied in a server-planned upload unit.",
  "properties": {
    "artifact_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Artifact Sha256",
      "type": "string"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "offset": {
      "minimum": 0,
      "title": "Offset",
      "type": "integer"
    },
    "path": {
      "title": "Path",
      "type": "string"
    }
  },
  "required": [
    "path",
    "offset",
    "bytes",
    "artifact_sha256"
  ],
  "title": "CollectionUploadUnitSourceDocument",
  "type": "object"
}
```
