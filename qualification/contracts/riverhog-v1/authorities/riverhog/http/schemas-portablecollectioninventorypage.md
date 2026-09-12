# schemas: PortableCollectionInventoryPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectioninventorypage:e52591cde2 -->

One bounded, canonically ordered slice of an immutable inventory.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-bf924c1cdf"></a>
- <a id="s-3d12d079b3"></a>`title`: PortableCollectionInventoryPage
- <a id="s-f6a715c2a9"></a>`description`: One bounded, canonically ordered slice of an immutable inventory.
- <a id="s-fee778a134"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fda883ea0a"></a>`authority` | yes | #/components/schemas/PortableCollectionInventoryAuthority |  |
| <a id="s-88b00618b6"></a>`complete` | yes | type="boolean" |  |
| <a id="s-6b5d814482"></a>`files` | yes | type="array"; maxItems=1000; items=(#/components/schemas/ImmutableFileIdentityDocument); additional keys=`x-riverhog-extent` |  |
| <a id="s-15a2db977b"></a>`format` | no | type="string"; const="riverhog-collection-inventory-page/v1" |  |
| <a id="s-b6881aa3a4"></a>`next_cursor` | no | anyOf=type="string"; minLength=1; maxLength=8192 \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: maximum=1000; progression={"authority":"portable-collection-inventory","cursor_parameter":"cursor","kind":"exact-set-page","limit_parameter":"limit","validator_header":"If-Match"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-6b5d814482) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=8192; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-731e856a17"></a>[field next_cursor · string value](#s-b6881aa3a4) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ImmutableFileIdentityDocument](schemas-immutablefileidentitydocument.md)
- [schemas: PortableCollectionInventoryAuthority](schemas-portablecollectioninventoryauthority.md)

## Governing policies

- <a id="pa-68d97581aa"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-e5b2bdc5d1"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-cc2373473c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionInventoryPage`

### Exact owned JSON

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
