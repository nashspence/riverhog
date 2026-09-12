# schemas: CollectionUploadRawDigestBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadrawdigestbatchdocument:088760615c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawDigestBatchDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`
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
| cardinality | items | `segmented_no_total_max` | maximum=1024, minimum=1, reason=bounded-raw-digest-append |

## Contract summary

- `title`: CollectionUploadRawDigestBatchDocument
- `description`: One append-only bounded slice of a registered raw source digest sequence.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `first_part` | yes | integer |  |
| `path` | yes | string |  |
| `sha256s` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3e9c783bf5afec4ecfc01d05a15564e73cead5a37ad60e657d59c0a4e586f01 -->

```json
{
  "additionalProperties": false,
  "description": "One append-only bounded slice of a registered raw source digest sequence.",
  "properties": {
    "first_part": {
      "minimum": 0,
      "title": "First Part",
      "type": "integer"
    },
    "path": {
      "title": "Path",
      "type": "string"
    },
    "sha256s": {
      "items": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      },
      "maxItems": 1024,
      "minItems": 1,
      "title": "Sha256S",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "first_part",
        "reason": "bounded-raw-digest-append"
      }
    }
  },
  "required": [
    "path",
    "first_part",
    "sha256s"
  ],
  "title": "CollectionUploadRawDigestBatchDocument",
  "type": "object"
}
```
