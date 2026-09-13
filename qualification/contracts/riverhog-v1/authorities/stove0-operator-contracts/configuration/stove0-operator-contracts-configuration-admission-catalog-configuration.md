# stove0-operator-contracts:configuration:admission-catalog configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-operator-contracts:stove0-operator-contracts-configuration-a-feb6a2fb56:6a6f99229e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Configuration Documents](index.md) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-dca869a611"></a>
- <a id="s-84eb95d112"></a>`title`: AdmissionCatalog
- <a id="s-ddaf3fc165"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11e92b75d6"></a>`format` | no | type="string"; const="stove0-admissions/v1" |  |
| <a id="s-be60a069b3"></a>`policies` | no | type="array"; maxItems=100; items=(#/$defs/AdmissionPolicy); additional keys=`x-riverhog-extent` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-a16490ee43"></a>`AdmissionPolicy` | type="object"; fields=`automatic_preview`, `effective_intent`, `format`, `id`, `recipe_id`, `recipe_revision`, `recipe_sha256`, `required_tags`, `revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-126d750bd7"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-c9ee6fcc4b"></a>`JsonValue` | empty object |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-operator-contracts:configuration:admission-catalog"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-1d7d127388"></a>[definition AdmissionPolicy · field effective_intent](#s-a16490ee43) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-a9af99ca70"></a>[definition AdmissionPolicy · field recipe_id](#s-a16490ee43) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| <a id="s-154969045e"></a>[definition AdmissionPolicy · field recipe_sha256](#s-a16490ee43) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-33d838d1b6"></a>[definition AdmissionPolicy · field required_tags](#s-a16490ee43) | `cardinality · items · contract_max` | maximum=100; minimum=1; reason="bounded-exact-classification-admission-predicate" |
| [definition CollectionTag](#s-126d750bd7) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-126d750bd7) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| [field policies](#s-be60a069b3) | `cardinality · items · contract_max` | maximum=100; reason="bounded-deployment-admission-catalog" |

## Governing policies

- <a id="pa-dd2b0b51de"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-4636ef56db"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)
- <a id="pa-126ab7c383"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-operator-contracts:configuration:admission-catalog](../../../evidence/sources.md#src-c0d7e75302) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py::AdmissionCatalog`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/stove0-operator-contracts:configuration:admission-catalog`

### Exact owned JSON

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
