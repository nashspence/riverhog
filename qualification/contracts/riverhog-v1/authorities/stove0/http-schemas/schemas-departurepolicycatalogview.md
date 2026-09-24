# schemas: DeparturePolicyCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-departurepolicycatalogview:b972dad11d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-862f8cbfd0"></a>

- <a id="s-6dcfae1683"></a>`type`: `"object"`
- <a id="s-e0ee8f167f"></a>`additionalProperties`: `false`
- <a id="s-69b141250e"></a>`required`: `["catalog_sha256","policies"]`
- <a id="s-f7608e97bd"></a>`title`: `"DeparturePolicyCatalogView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4cd7e7cb09"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Catalog Sha256" |  |
| <a id="s-7c8cbf7048"></a>`policies` | yes | type="array"; items=([DeparturePolicyStatus](schemas-departurepolicystatus.md)); title="Policies" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field policies](#s-7c8cbf7048) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field catalog_sha256](#s-4cd7e7cb09) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [DeparturePolicyStatus](schemas-departurepolicystatus.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d11e8d2118"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-f93fa94096"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-d80dc9fff5"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/DeparturePolicyCatalogView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09ca0aaf8c6ea9f7686fdc6ae103a36b289f35f37ce0b357d6f2ee4052b219ee -->

```json
{
  "additionalProperties": false,
  "properties": {
    "catalog_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Catalog Sha256",
      "type": "string"
    },
    "policies": {
      "items": {
        "$ref": "#/components/schemas/DeparturePolicyStatus"
      },
      "title": "Policies",
      "type": "array"
    }
  },
  "required": [
    "catalog_sha256",
    "policies"
  ],
  "title": "DeparturePolicyCatalogView",
  "type": "object"
}
```

</details>
