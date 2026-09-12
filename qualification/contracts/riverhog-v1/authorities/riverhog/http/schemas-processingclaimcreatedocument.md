# schemas: ProcessingClaimCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimcreatedocument:5abc67569d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimCreateDocument`

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
| value | schema-value | `contract_max` | maximum=86400, minimum=30, reason=schema-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=4194304, reason=bounded-work-document-envelope |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ProcessingClaimCreateDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `lease_seconds` | no | integer |  |
| `purpose` | no | string |  |
| `work_document` | yes | object |  |
| `work_document_sha256` | yes | string |  |
| `work_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f81f54843c68bd43b662283b523cf02085ec0e8d673cd66cdb5201223ea00a6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "lease_seconds": {
      "default": 1800,
      "maximum": 86400,
      "minimum": 30,
      "title": "Lease Seconds",
      "type": "integer"
    },
    "purpose": {
      "default": "collection-work/v1",
      "maxLength": 160,
      "minLength": 1,
      "title": "Purpose",
      "type": "string"
    },
    "work_document": {
      "additionalProperties": true,
      "title": "Work Document",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 4194304,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-work-document-envelope"
      }
    },
    "work_document_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Document Sha256",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "work_id",
    "work_document",
    "work_document_sha256"
  ],
  "title": "ProcessingClaimCreateDocument",
  "type": "object"
}
```
