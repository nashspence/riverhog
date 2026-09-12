# schemas: PendingCollectionUploadCustodyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-pendingcollectionuploadcustodyout:d5f9864287 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PendingCollectionUploadCustodyOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: PendingCollectionUploadCustodyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `bytes` | yes | integer |  |
| `files` | yes | integer |  |
| `state` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bba3c06332a578b2b21e4382d6c7fffbc08107d4af02df1ec79ed7e4a839cbb7 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "state": {
      "const": "pending",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state",
    "files",
    "bytes"
  ],
  "title": "PendingCollectionUploadCustodyOut",
  "type": "object"
}
```
