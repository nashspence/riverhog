# schemas: CollectionTagMembershipOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiontagmembershipout:3f900f4648 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMembershipOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionTag](schemas-collectiontag.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CollectionTagMembershipOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `present` | yes | boolean |  |
| `revision` | yes | integer |  |
| `tag` | yes | #/components/schemas/CollectionTag |  |
| `tag_set_identity` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5738ba195894d75eda6ed2223cfb464e17eaa62270678415546f6c0f887dd89f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "present": {
      "title": "Present",
      "type": "boolean"
    },
    "revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
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
    "revision",
    "tag_set_identity",
    "tag",
    "present"
  ],
  "title": "CollectionTagMembershipOut",
  "type": "object"
}
```
