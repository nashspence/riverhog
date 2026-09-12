# schemas: CollectionDescriptionOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondescriptionout:46831cf4ef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-b416730527"></a>
- <a id="s-bc75f9b96b"></a>`title`: CollectionDescriptionOut
- <a id="s-9a95548ab9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff89eed8dd"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-fad18f5e39"></a>`description` | yes | anyOf=#/components/schemas/CollectionDescription \| type="null" |  |
| <a id="s-ed8b188dda"></a>`description_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fb5f576279"></a>`description_publication` | yes | type="string"; enum=["not_required","current","reconciling"] |  |
| <a id="s-cdcae3a251"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field description_identity](#s-ed8b188dda) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field description_revision](#s-cdcae3a251) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDescription](schemas-collectiondescription.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-931c064888"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-781e90c2aa"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDescriptionOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a06a3e3817156e13f2bf603f5f26c314164e46195b2ea22f1121fc4bbc1ed24f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    },
    "description_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Description Identity",
      "type": "string"
    },
    "description_publication": {
      "enum": [
        "not_required",
        "current",
        "reconciling"
      ],
      "title": "Description Publication",
      "type": "string"
    },
    "description_revision": {
      "maximum": 9007199254740991,
      "minimum": 0,
      "title": "Description Revision",
      "type": "integer"
    }
  },
  "required": [
    "collection_id",
    "description",
    "description_revision",
    "description_identity",
    "description_publication"
  ],
  "title": "CollectionDescriptionOut",
  "type": "object"
}
```
