# schemas: CollectionDescriptionOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiondescriptionout:119878cc3a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b416730527"></a>

- <a id="s-9a95548ab9"></a>`type`: `"object"`
- <a id="s-e76bb01194"></a>`additionalProperties`: `false`
- <a id="s-0e7ba20066"></a>`required`: `["collection_id","description","description_revision","description_identity","description_publication"]`
- <a id="s-bc75f9b96b"></a>`title`: `"CollectionDescriptionOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff89eed8dd"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-fad18f5e39"></a>`description` | yes | anyOf=[([CollectionDescription](schemas-collectiondescription.md)); (type="null")] |  |
| <a id="s-ed8b188dda"></a>`description_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Description Identity" |  |
| <a id="s-fb5f576279"></a>`description_publication` | yes | type="string"; enum=["not_required","current","reconciling"]; title="Description Publication" |  |
| <a id="s-cdcae3a251"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991; title="Description Revision" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field description_identity](#s-ed8b188dda) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field description_revision](#s-cdcae3a251) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [CollectionDescription](schemas-collectiondescription.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-997790d0b8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e59df476fc"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDescriptionOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
