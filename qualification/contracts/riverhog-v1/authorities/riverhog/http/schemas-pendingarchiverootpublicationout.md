# schemas: PendingArchiveRootPublicationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-pendingarchiverootpublicationout:d6504c7b47 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: PendingArchiveRootPublicationOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `object_path` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| `sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| `state` | no | type="string"; const="pending" |  |

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

- `/external_contract/http_openapi/riverhog/components/schemas/PendingArchiveRootPublicationOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 270f8f453df5bf9a03a87bc68ad94f0f932b5a11c6974579ec7bd7809c18a960 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "object_path": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Object Path"
    },
    "sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Sha256"
    },
    "state": {
      "const": "pending",
      "default": "pending",
      "title": "State",
      "type": "string"
    }
  },
  "title": "PendingArchiveRootPublicationOut",
  "type": "object"
}
```
