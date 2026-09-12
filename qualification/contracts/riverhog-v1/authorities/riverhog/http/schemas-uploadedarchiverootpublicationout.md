# schemas: UploadedArchiveRootPublicationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-uploadedarchiverootpublicationout:a75186572a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: UploadedArchiveRootPublicationOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `object_path` | yes | type="string"; minLength=1 |  |
| `sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `state` | yes | type="string"; const="uploaded" |  |

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
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/UploadedArchiveRootPublicationOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09c985b2a04103fb9074371778c6f08cf50ee3306070442f52193a4f7e8db5cf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "object_path": {
      "minLength": 1,
      "title": "Object Path",
      "type": "string"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    },
    "state": {
      "const": "uploaded",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "object_path",
    "sha256",
    "state"
  ],
  "title": "UploadedArchiveRootPublicationOut",
  "type": "object"
}
```
