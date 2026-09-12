# schemas: PendingArchiveRootPublicationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-pendingarchiverootpublicationout:d6504c7b47 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PendingArchiveRootPublicationOut`

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

- `title`: PendingArchiveRootPublicationOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `object_path` | no | object (2 fields) |  |
| `sha256` | no | object (2 fields) |  |
| `state` | no | string |  |

## Complete owned contract

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
