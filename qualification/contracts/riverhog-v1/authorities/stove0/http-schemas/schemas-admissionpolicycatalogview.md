# schemas: AdmissionPolicyCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-admissionpolicycatalogview:12d5ccc1df -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3ced3fdf8f"></a>

- <a id="s-7c2852f540"></a>`type`: `"object"`
- <a id="s-a7b0b2bce2"></a>`additionalProperties`: `false`
- <a id="s-cac21d588a"></a>`required`: `["catalog_sha256","policies"]`
- <a id="s-bebdb2fa63"></a>`title`: `"AdmissionPolicyCatalogView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a131527fd"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Catalog Sha256" |  |
| <a id="s-aa7ac7c468"></a>`policies` | yes | type="array"; items=([AdmissionPolicyStatus](schemas-admissionpolicystatus.md)); title="Policies" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field policies](#s-aa7ac7c468) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field catalog_sha256](#s-4a131527fd) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [AdmissionPolicyStatus](schemas-admissionpolicystatus.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9f93330b37"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3503832715"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-ec940faf1f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPolicyCatalogView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04298da17774d7c5d89b5b02199638603037f7be9d0a675d5e4ca71ff7612af8 -->

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
        "$ref": "#/components/schemas/AdmissionPolicyStatus"
      },
      "title": "Policies",
      "type": "array"
    }
  },
  "required": [
    "catalog_sha256",
    "policies"
  ],
  "title": "AdmissionPolicyCatalogView",
  "type": "object"
}
```

</details>
