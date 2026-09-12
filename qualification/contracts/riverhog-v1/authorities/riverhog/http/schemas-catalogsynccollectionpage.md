# schemas: CatalogSyncCollectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsynccollectionpage:53f21f4094 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-5316dd1cc7"></a>
- <a id="s-2745abeda4"></a>`title`: CatalogSyncCollectionPage
- <a id="s-5d94d54247"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-809ff09438"></a>`authorization_view_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1d9219fe71"></a>`changes_cursor` | no | anyOf=type="string"; minLength=1; maxLength=4096 \| type="null" |  |
| <a id="s-4cd0865d3f"></a>`collections` | yes | type="array"; maxItems=100; items=(#/components/schemas/CatalogSyncDescriptor) |  |
| <a id="s-43a0a9899d"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1" |  |
| <a id="s-908ace0fa8"></a>`next_cursor` | no | anyOf=type="string"; minLength=1; maxLength=4096 \| type="null" |  |
| <a id="s-c897f48ff8"></a>`source_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: maximum=100; progression={"authority":"catalog-sync-bootstrap","cursor_parameter":"cursor","kind":"exact-authority-page","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collections](#s-4cd0865d3f) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field authorization_view_identity](#s-809ff09438) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| <a id="s-699f55310e"></a>[field changes_cursor · string value](#s-1d9219fe71) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-2169d7062f"></a>[field next_cursor · string value](#s-908ace0fa8) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field source_identity](#s-c897f48ff8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md)

## Governing policies

- <a id="pa-0486095a72"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-6798604038"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-dfcffb5015"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncCollectionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f827ad0f12c48d5d82537082dfcdf9301e028023919f79fbce029e31b3013ab -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authorization_view_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Authorization View Identity",
      "type": "string"
    },
    "changes_cursor": {
      "anyOf": [
        {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Changes Cursor"
    },
    "collections": {
      "items": {
        "$ref": "#/components/schemas/CatalogSyncDescriptor"
      },
      "maxItems": 100,
      "title": "Collections",
      "type": "array"
    },
    "format": {
      "const": "riverhog-catalog-sync/v1",
      "default": "riverhog-catalog-sync/v1",
      "title": "Format",
      "type": "string"
    },
    "next_cursor": {
      "anyOf": [
        {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Cursor"
    },
    "source_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Identity",
      "type": "string"
    }
  },
  "required": [
    "source_identity",
    "authorization_view_identity",
    "collections"
  ],
  "title": "CatalogSyncCollectionPage",
  "type": "object"
}
```
