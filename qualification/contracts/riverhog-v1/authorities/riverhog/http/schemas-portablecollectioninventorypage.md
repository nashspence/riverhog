# schemas: PortableCollectionInventoryPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectioninventorypage:e52591cde2 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionInventoryPage`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ImmutableFileIdentityDocument](schemas-immutablefileidentitydocument.md)
- [schemas: PortableCollectionInventoryAuthority](schemas-portablecollectioninventoryauthority.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=1000, reason=bounded-route-page |
| length | characters | `contract_max` | maximum=8192, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: PortableCollectionInventoryPage
- `description`: One bounded, canonically ordered slice of an immutable inventory.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authority` | yes | #/components/schemas/PortableCollectionInventoryAuthority |  |
| `complete` | yes | boolean |  |
| `files` | yes | array |  |
| `format` | no | string |  |
| `next_cursor` | no | object (2 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c391f6247e6df02e588c62fb69f461cc6b0976d24b07a65ed33578c67969860 -->

```json
{
  "additionalProperties": false,
  "description": "One bounded, canonically ordered slice of an immutable inventory.",
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/PortableCollectionInventoryAuthority"
    },
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/ImmutableFileIdentityDocument"
      },
      "maxItems": 1000,
      "title": "Files",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-cursor",
        "reason": "bounded-portable-inventory-page"
      }
    },
    "format": {
      "const": "riverhog-collection-inventory-page/v1",
      "default": "riverhog-collection-inventory-page/v1",
      "title": "Format",
      "type": "string"
    },
    "next_cursor": {
      "anyOf": [
        {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Cursor"
    }
  },
  "required": [
    "authority",
    "files",
    "complete"
  ],
  "title": "PortableCollectionInventoryPage",
  "type": "object"
}
```
