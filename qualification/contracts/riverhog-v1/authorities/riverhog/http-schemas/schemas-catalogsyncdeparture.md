# schemas: CatalogSyncDeparture

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-catalogsyncdeparture:cd194a756b -->

A collection left this view; the cause identifies deletion or visibility loss.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f7f546cbe1"></a>

- <a id="s-127e8ec317"></a>`type`: `"object"`
- <a id="s-d959a9ddd3"></a>`additionalProperties`: `false`
- <a id="s-1e8ebda4cb"></a>`description`: `"A collection left this view; the cause identifies deletion or visibility loss."`
- <a id="s-6152dbfab3"></a>`required`: `["cause","collection_id","revision"]`
- <a id="s-10d50f15b6"></a>`title`: `"CatalogSyncDeparture"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ec11b44ba"></a>`cause` | yes | type="string"; enum=["collection_deleted","visibility_lost"]; title="Cause" |  |
| <a id="s-02ba909d41"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-ab70ac6067"></a>`operation` | no | type="string"; const="departure"; default="departure"; title="Operation" |  |
| <a id="s-8d68297138"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$"; title="Revision" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=19; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field revision](#s-8d68297138) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0104422df9"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4759f600a3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncDeparture`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 894cd8d85cab2abfcd37c9107dcd0e7742ad754905903886354dad49c76d80bd -->

```json
{
  "additionalProperties": false,
  "description": "A collection left this view; the cause identifies deletion or visibility loss.",
  "properties": {
    "cause": {
      "enum": [
        "collection_deleted",
        "visibility_lost"
      ],
      "title": "Cause",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "operation": {
      "const": "departure",
      "default": "departure",
      "title": "Operation",
      "type": "string"
    },
    "revision": {
      "maxLength": 19,
      "minLength": 1,
      "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
      "title": "Revision",
      "type": "string"
    }
  },
  "required": [
    "cause",
    "collection_id",
    "revision"
  ],
  "title": "CatalogSyncDeparture",
  "type": "object"
}
```

</details>
