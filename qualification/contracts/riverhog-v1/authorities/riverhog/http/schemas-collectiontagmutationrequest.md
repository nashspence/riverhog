# schemas: CollectionTagMutationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiontagmutationrequest:df7856e0e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: CollectionTagMutationRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `expected_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| `expected_tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `operation_id` | yes | type="string"; minLength=1; maxLength=256; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| `tag` | yes | #/components/schemas/CollectionTag |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=256, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMutationRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c1ccadd087c4e2e2878336b5365711d11b7682d9cd606b8574e8b6c4bfbe27b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "expected_revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Expected Revision",
      "type": "integer"
    },
    "expected_tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Expected Tag Set Identity",
      "type": "string"
    },
    "operation_id": {
      "maxLength": 256,
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Operation Id",
      "type": "string"
    },
    "tag": {
      "$ref": "#/components/schemas/CollectionTag"
    }
  },
  "required": [
    "operation_id",
    "tag",
    "expected_revision",
    "expected_tag_set_identity"
  ],
  "title": "CollectionTagMutationRequest",
  "type": "object"
}
```
