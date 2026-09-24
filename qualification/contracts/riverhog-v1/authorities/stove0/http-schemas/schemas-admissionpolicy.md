# schemas: AdmissionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-admissionpolicy:ff76b0b846 -->

One bounded admission rule over the policy's Riverhog authorization view.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-9dd4f15559"></a>

- <a id="s-05470c8cac"></a>`type`: `"object"`
- <a id="s-f96d685d70"></a>`additionalProperties`: `false`
- <a id="s-e714fd9d0a"></a>`description`: `"One bounded admission rule over the policy's Riverhog authorization view."`
- <a id="s-8b6c4855c7"></a>`required`: `["id","revision","selector","recipe_id","recipe_revision","recipe_sha256"]`
- <a id="s-99894eeca3"></a>`title`: `"AdmissionPolicy"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4825bdded5"></a>`automatic_preview` | no | type="string"; const="accept-ready"; default="accept-ready"; title="Automatic Preview" |  |
| <a id="s-fe0784373c"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Effective Intent" |  |
| <a id="s-7ca1adfe1d"></a>`format` | no | type="string"; const="stove0-admission-policy/v1"; default="stove0-admission-policy/v1"; title="Format" |  |
| <a id="s-f6d577c8df"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-fff97fd387"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-f81203e699"></a>`recipe_revision` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-59bbce29c6"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Recipe Sha256" |  |
| <a id="s-558e3f83fb"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-2cb7a844c5"></a>`selector` | yes | discriminator={"mapping":{"all":"#/components/schemas/AllVisibleAdmissionSelector","tags":"#/components/schemas/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](schemas-allvisibleadmissionselector.md)); ([TaggedAdmissionSelector](schemas-taggedadmissionselector.md))]; title="Selector" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-fe0784373c) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field recipe_id](#s-fff97fd387) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field recipe_sha256](#s-59bbce29c6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [AllVisibleAdmissionSelector](schemas-allvisibleadmissionselector.md)
- [JsonValue](schemas-jsonvalue.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [TaggedAdmissionSelector](schemas-taggedadmissionselector.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-02242020d8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-72a9e121f5"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-793f29d9f1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPolicy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a62a50eba6b6b67536cf8c3cf6bd2c95506c93265e7e597cfb79083e8824542 -->

```json
{
  "additionalProperties": false,
  "description": "One bounded admission rule over the policy's Riverhog authorization view.",
  "properties": {
    "automatic_preview": {
      "const": "accept-ready",
      "default": "accept-ready",
      "title": "Automatic Preview",
      "type": "string"
    },
    "effective_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Effective Intent",
      "type": "object"
    },
    "format": {
      "const": "stove0-admission-policy/v1",
      "default": "stove0-admission-policy/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "recipe_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Recipe Id",
      "type": "string"
    },
    "recipe_revision": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "recipe_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Recipe Sha256",
      "type": "string"
    },
    "revision": {
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "selector": {
      "discriminator": {
        "mapping": {
          "all": "#/components/schemas/AllVisibleAdmissionSelector",
          "tags": "#/components/schemas/TaggedAdmissionSelector"
        },
        "propertyName": "kind"
      },
      "oneOf": [
        {
          "$ref": "#/components/schemas/AllVisibleAdmissionSelector"
        },
        {
          "$ref": "#/components/schemas/TaggedAdmissionSelector"
        }
      ],
      "title": "Selector"
    }
  },
  "required": [
    "id",
    "revision",
    "selector",
    "recipe_id",
    "recipe_revision",
    "recipe_sha256"
  ],
  "title": "AdmissionPolicy",
  "type": "object"
}
```

</details>
