# schemas: CatalogSyncChangePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-catalogsyncchangepage:ecb590946e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ddd44c6b0a"></a>

- <a id="s-5b72cd0e42"></a>`type`: `"object"`
- <a id="s-3e646cb13d"></a>`additionalProperties`: `false`
- <a id="s-de3f59d8de"></a>`required`: `["source_identity","authorization_view_identity","changes","next_cursor","caught_up","through_revision"]`
- <a id="s-878643edf9"></a>`title`: `"CatalogSyncChangePage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8871b88c1f"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Authorization View Identity" |  |
| <a id="s-432e1a3267"></a>`caught_up` | yes | type="boolean"; title="Caught Up" |  |
| <a id="s-6ab6578eda"></a>`changes` | yes | type="array"; items=(discriminator={"mapping":{"delete":"#/components/schemas/CatalogSyncDelete","upsert":"#/components/schemas/CatalogSyncUpsert"},"propertyName":"operation"}; oneOf=[([CatalogSyncUpsert](schemas-catalogsyncupsert.md)); ([CatalogSyncDelete](schemas-catalogsyncdelete.md))]); maxItems=100; title="Changes" |  |
| <a id="s-7b7de7e14d"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1"; default="riverhog-catalog-sync/v1"; title="Format" |  |
| <a id="s-e059c8b7fc"></a>`next_cursor` | yes | type="string"; maxLength=4096; minLength=1; title="Next Cursor" |  |
| <a id="s-aad3b69d89"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Source Identity" |  |
| <a id="s-7679cb53ca"></a>`through_revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18})$"; title="Through Revision" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=100; progression={"cursor_parameter":"cursor","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field changes](#s-6ab6578eda) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field authorization_view_identity](#s-8871b88c1f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field next_cursor](#s-e059c8b7fc) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field source_identity](#s-aad3b69d89) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field through_revision](#s-7679cb53ca) | `length · characters · contract_max` | maximum=19; minimum=1; reason="schema-maximum" |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [CatalogSyncDelete](schemas-catalogsyncdelete.md)
- [CatalogSyncUpsert](schemas-catalogsyncupsert.md)

## Governing policies

- <a id="pa-0d7f90d5d4"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-f541c79a11"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-c83fa38e1f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncChangePage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
