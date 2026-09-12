# schemas: CatalogSyncChangePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsyncchangepage:a10a7f5278 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-ddd44c6b0a"></a>
- <a id="s-878643edf9"></a>`title`: CatalogSyncChangePage
- <a id="s-5b72cd0e42"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8871b88c1f"></a>`authorization_view_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-432e1a3267"></a>`caught_up` | yes | type="boolean" |  |
| <a id="s-6ab6578eda"></a>`changes` | yes | type="array"; maxItems=100; items=(oneOf=#/components/schemas/CatalogSyncUpsert \| #/components/schemas/CatalogSyncDelete; additional keys=`discriminator`) |  |
| <a id="s-7b7de7e14d"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1" |  |
| <a id="s-e059c8b7fc"></a>`next_cursor` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-aad3b69d89"></a>`source_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7679cb53ca"></a>`through_revision` | yes | type="string"; minLength=1; maxLength=19; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: maximum=100; progression={"cursor_parameter":"cursor","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field changes](#s-6ab6578eda) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field authorization_view_identity](#s-8871b88c1f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field next_cursor](#s-e059c8b7fc) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field source_identity](#s-aad3b69d89) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field through_revision](#s-7679cb53ca) | `length · characters · contract_max` | maximum=19; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CatalogSyncDelete](schemas-catalogsyncdelete.md)
- [schemas: CatalogSyncUpsert](schemas-catalogsyncupsert.md)

## Governing policies

- <a id="pa-add08879bd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-684da17309"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-5400af5de3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncChangePage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9134d3884d10ad80c358ca088ef4cb1983554941cd38bf93eb6e4c22d95cd09 -->

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
    "caught_up": {
      "title": "Caught Up",
      "type": "boolean"
    },
    "changes": {
      "items": {
        "discriminator": {
          "mapping": {
            "delete": "#/components/schemas/CatalogSyncDelete",
            "upsert": "#/components/schemas/CatalogSyncUpsert"
          },
          "propertyName": "operation"
        },
        "oneOf": [
          {
            "$ref": "#/components/schemas/CatalogSyncUpsert"
          },
          {
            "$ref": "#/components/schemas/CatalogSyncDelete"
          }
        ]
      },
      "maxItems": 100,
      "title": "Changes",
      "type": "array"
    },
    "format": {
      "const": "riverhog-catalog-sync/v1",
      "default": "riverhog-catalog-sync/v1",
      "title": "Format",
      "type": "string"
    },
    "next_cursor": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Next Cursor",
      "type": "string"
    },
    "source_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Identity",
      "type": "string"
    },
    "through_revision": {
      "maxLength": 19,
      "minLength": 1,
      "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
      "title": "Through Revision",
      "type": "string"
    }
  },
  "required": [
    "source_identity",
    "authorization_view_identity",
    "changes",
    "next_cursor",
    "caught_up",
    "through_revision"
  ],
  "title": "CatalogSyncChangePage",
  "type": "object"
}
```
