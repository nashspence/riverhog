# schemas: CollectionUploadUnitSourceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitsourcedocument:9513787822 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitSourceDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
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
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: CollectionUploadUnitSourceDocument
- `description`: One exact source range supplied in a server-planned upload unit.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_sha256` | yes | string |  |
| `bytes` | yes | integer |  |
| `offset` | yes | integer |  |
| `path` | yes | string |  |

## Complete owned contract

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
