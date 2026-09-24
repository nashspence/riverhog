# schemas: AdmissionIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-admissionintent:b0ea7ef367 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ba0530c236"></a>

- <a id="s-50460f4d51"></a>`type`: `"object"`
- <a id="s-5dd1a94947"></a>`additionalProperties`: `false`
- <a id="s-1b356cffeb"></a>`required`: `["admission_id","policy_id","policy_revision","policy_sha256","required_tags","collection","recipe_id","recipe_revision","recipe_sha256","effective_intent"]`
- <a id="s-0aec38de14"></a>`title`: `"AdmissionIntent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-edd0a3adb5"></a>`admission_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Admission Id" |  |
| <a id="s-0bad523de4"></a>`collection` | yes | [CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md) |  |
| <a id="s-4b7ce35bca"></a>`effective_intent` | yes | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Effective Intent" |  |
| <a id="s-ec404034ae"></a>`format` | no | type="string"; const="stove0-admission-intent/v1"; default="stove0-admission-intent/v1"; title="Format" |  |
| <a id="s-33c67405d9"></a>`policy_id` | yes | type="string"; maxLength=160; minLength=1; title="Policy Id" |  |
| <a id="s-f4f9002e65"></a>`policy_revision` | yes | type="integer"; minimum=1; title="Policy Revision" |  |
| <a id="s-75bb897925"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Policy Sha256" |  |
| <a id="s-4ff09b93ec"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-b4e40a831d"></a>`recipe_revision` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-17ec6189a3"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Recipe Sha256" |  |
| <a id="s-81dfbfb6bf"></a>`required_tags` | yes | type="array"; items=([CollectionTag](schemas-collectiontag.md)); title="Required Tags" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-4b7ce35bca) | `cardinality · entries · operational_policy` | shared above |
| [field required_tags](#s-81dfbfb6bf) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field admission_id](#s-edd0a3adb5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field policy_id](#s-33c67405d9) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field policy_sha256](#s-75bb897925) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field recipe_id](#s-4ff09b93ec) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field recipe_sha256](#s-17ec6189a3) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md)
- [CollectionTag](schemas-collectiontag.md)
- [JsonValue](schemas-jsonvalue.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-351b5364db"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3247581e4b"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-3eba2af874"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33d1b1c45c3f3fb6815907a5f4776bf89b4497d9a706e5450a4268306e0b15ea -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admission_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Admission Id",
      "type": "string"
    },
    "collection": {
      "$ref": "#/components/schemas/CatalogSyncDescriptor"
    },
    "effective_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Effective Intent",
      "type": "object"
    },
    "format": {
      "const": "stove0-admission-intent/v1",
      "default": "stove0-admission-intent/v1",
      "title": "Format",
      "type": "string"
    },
    "policy_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Policy Id",
      "type": "string"
    },
    "policy_revision": {
      "minimum": 1,
      "title": "Policy Revision",
      "type": "integer"
    },
    "policy_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Policy Sha256",
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
    "required_tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "title": "Required Tags",
      "type": "array"
    }
  },
  "required": [
    "admission_id",
    "policy_id",
    "policy_revision",
    "policy_sha256",
    "required_tags",
    "collection",
    "recipe_id",
    "recipe_revision",
    "recipe_sha256",
    "effective_intent"
  ],
  "title": "AdmissionIntent",
  "type": "object"
}
```

</details>
