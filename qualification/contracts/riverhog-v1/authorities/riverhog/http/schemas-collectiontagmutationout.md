# schemas: CollectionTagMutationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiontagmutationout:6300d19241 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: CollectionTagMutationOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `action` | yes | type="string"; enum=["add","remove"] |  |
| `changed` | yes | type="boolean" |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `head_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `operation_id` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| `revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| `root_sha256` | yes | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| `state` | yes | type="string"; enum=["pending","retry_wait","succeeded"] |  |
| `tag` | yes | #/components/schemas/CollectionTag |  |
| `tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
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

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMutationOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b998f00d5a88ea2831690b43733ac89b267e4a3befa8f624f559bcf642a0509 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "action": {
      "enum": [
        "add",
        "remove"
      ],
      "title": "Action",
      "type": "string"
    },
    "changed": {
      "title": "Changed",
      "type": "boolean"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "head_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Head Identity",
      "type": "string"
    },
    "operation_id": {
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Operation Id",
      "type": "string"
    },
    "revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "root_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Root Sha256"
    },
    "state": {
      "enum": [
        "pending",
        "retry_wait",
        "succeeded"
      ],
      "title": "State",
      "type": "string"
    },
    "tag": {
      "$ref": "#/components/schemas/CollectionTag"
    },
    "tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Tag Set Identity",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "operation_id",
    "action",
    "tag",
    "changed",
    "revision",
    "root_sha256",
    "tag_set_identity",
    "head_identity",
    "state"
  ],
  "title": "CollectionTagMutationOut",
  "type": "object"
}
```
