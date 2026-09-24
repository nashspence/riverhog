# schemas: CatalogSyncCollectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-catalogsynccollectionpage:23abf7267b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5316dd1cc7"></a>

- <a id="s-5d94d54247"></a>`type`: `"object"`
- <a id="s-387df8c437"></a>`additionalProperties`: `false`
- <a id="s-622f3d1259"></a>`required`: `["source_identity","authorization_view_identity","collections"]`
- <a id="s-2745abeda4"></a>`title`: `"CatalogSyncCollectionPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-809ff09438"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Authorization View Identity" |  |
| <a id="s-1d9219fe71"></a>`changes_cursor` | no | anyOf=[(type="string"; maxLength=4096; minLength=1); (type="null")]; title="Changes Cursor" |  |
| <a id="s-4cd0865d3f"></a>`collections` | yes | type="array"; items=([CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md)); maxItems=100; title="Collections" |  |
| <a id="s-43a0a9899d"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1"; default="riverhog-catalog-sync/v1"; title="Format" |  |
| <a id="s-908ace0fa8"></a>`next_cursor` | no | anyOf=[(type="string"; maxLength=4096; minLength=1); (type="null")]; title="Next Cursor" |  |
| <a id="s-c897f48ff8"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Source Identity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=100; progression={"authority":"catalog-sync-bootstrap","cursor_parameter":"cursor","kind":"exact-authority-page","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collections](#s-4cd0865d3f) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field authorization_view_identity](#s-809ff09438) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| <a id="s-699f55310e"></a>[field changes_cursor · string value](#s-1d9219fe71) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-2169d7062f"></a>[field next_cursor · string value](#s-908ace0fa8) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field source_identity](#s-c897f48ff8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |

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

- [CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dce34a11ba"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-9c86f167f3"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-17853dd1a1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncCollectionPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
