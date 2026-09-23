# stove0-operator-contracts:configuration:admission-catalog configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-operator-contracts:stove0-operator-contracts-configuration-a-feb6a2fb56:6a6f99229e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-dca869a611"></a>

- <a id="s-ddaf3fc165"></a>`type`: `"object"`
- <a id="s-62986891cd"></a>`additionalProperties`: `false`
- <a id="s-84eb95d112"></a>`title`: `"AdmissionCatalog"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11e92b75d6"></a>`format` | no | type="string"; const="stove0-admissions/v1"; default="stove0-admissions/v1"; title="Format" |  |
| <a id="s-be60a069b3"></a>`policies` | no | type="array"; default=[]; items=([AdmissionPolicy](#s-a16490ee43)); maxItems=100; title="Policies"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-deployment-admission-catalog"} |  |

### Definitions

- [AdmissionPolicy](#s-a16490ee43)
- [CollectionTag](#s-126d750bd7)
- [JsonValue](#s-c9ee6fcc4b)

### <a id="s-a16490ee43"></a>definition `AdmissionPolicy`

- <a id="s-4edccb6349"></a>`type`: `"object"`
- <a id="s-d7b8c1c2a8"></a>`additionalProperties`: `false`
- <a id="s-645ae32ef8"></a>`description`: `"One bounded, exact all-of classification admission rule."`
- <a id="s-95cc912480"></a>`required`: `["id","revision","required_tags","recipe_id","recipe_revision","recipe_sha256"]`
- <a id="s-d37570a349"></a>`title`: `"AdmissionPolicy"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-49c4892c8a"></a>`automatic_preview` | no | type="string"; const="accept-ready"; default="accept-ready"; title="Automatic Preview" |  |
| <a id="s-1d7d127388"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-c9ee6fcc4b)); title="Effective Intent" |  |
| <a id="s-bc11e0be16"></a>`format` | no | type="string"; const="stove0-admission-policy/v1"; default="stove0-admission-policy/v1"; title="Format" |  |
| <a id="s-6a40b04299"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-a9af99ca70"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-12de946063"></a>`recipe_revision` | yes | type="integer"; minimum=1; title="Recipe Revision" |  |
| <a id="s-154969045e"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Recipe Sha256" |  |
| <a id="s-33d838d1b6"></a>`required_tags` | yes | type="array"; items=([CollectionTag](#s-126d750bd7)); maxItems=100; minItems=1; title="Required Tags"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |
| <a id="s-0849708c98"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |

### <a id="s-126d750bd7"></a>definition `CollectionTag`

- <a id="s-4dd1b679a5"></a>`type`: `"string"`
- <a id="s-1386137637"></a>`maxLength`: `65536`
- <a id="s-b2b3eab4fe"></a>`minLength`: `1`
- <a id="s-9852ce17d8"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-6638fef622"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-3a6cd01d55"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-c9ee6fcc4b"></a>definition `JsonValue`

- Accepts: any JSON value.

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-operator-contracts:configuration:admission-catalog"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition AdmissionPolicy · field effective_intent](#s-1d7d127388) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition AdmissionPolicy · field recipe_id](#s-a9af99ca70) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition AdmissionPolicy · field recipe_sha256](#s-154969045e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition AdmissionPolicy · field required_tags](#s-33d838d1b6) | `cardinality · items · contract_max` | maximum=100; minimum=1; reason="bounded-exact-classification-admission-predicate" |
| [definition CollectionTag](#s-126d750bd7) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-126d750bd7) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| [field policies](#s-be60a069b3) | `cardinality · items · contract_max` | maximum=100; reason="bounded-deployment-admission-catalog" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dd2b0b51de"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-4636ef56db"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-126ab7c383"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-operator-contracts:configuration:admission-catalog](../../../evidence/sources/authorities.md#src-c0d7e75302) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py::AdmissionCatalog](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/stove0-operator-contracts:configuration:admission-catalog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00ecac8a3c71b27fc2027d9df838959cdfe480be931fd7518ea0f785f84b01c0 -->

```json
{
  "$defs": {
    "AdmissionPolicy": {
      "additionalProperties": false,
      "description": "One bounded, exact all-of classification admission rule.",
      "properties": {
        "automatic_preview": {
          "const": "accept-ready",
          "default": "accept-ready",
          "title": "Automatic Preview",
          "type": "string"
        },
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
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
          "minimum": 1,
          "title": "Recipe Revision",
          "type": "integer"
        },
        "recipe_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Recipe Sha256",
          "type": "string"
        },
        "required_tags": {
          "items": {
            "$ref": "#/$defs/CollectionTag"
          },
          "maxItems": 100,
          "minItems": 1,
          "title": "Required Tags",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-exact-classification-admission-predicate"
          }
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
          "type": "integer"
        }
      },
      "required": [
        "id",
        "revision",
        "required_tags",
        "recipe_id",
        "recipe_revision",
        "recipe_sha256"
      ],
      "title": "AdmissionPolicy",
      "type": "object"
    },
    "CollectionTag": {
      "maxLength": 65536,
      "minLength": 1,
      "type": "string",
      "x-riverhog-encoded-bytes-max": 65536,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-human-authored-collection-tag"
      },
      "x-unicode-normalization": "NFC"
    },
    "JsonValue": {}
  },
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-admissions/v1",
      "default": "stove0-admissions/v1",
      "title": "Format",
      "type": "string"
    },
    "policies": {
      "default": [],
      "items": {
        "$ref": "#/$defs/AdmissionPolicy"
      },
      "maxItems": 100,
      "title": "Policies",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-deployment-admission-catalog"
      }
    }
  },
  "title": "AdmissionCatalog",
  "type": "object"
}
```

</details>
