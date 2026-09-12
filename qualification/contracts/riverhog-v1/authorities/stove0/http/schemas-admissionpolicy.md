# schemas: AdmissionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionpolicy:c8a66edb55 -->

One bounded, exact all-of classification admission rule.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-9dd4f15559b1"></a>
- <a id="s-99894eeca3a8"></a>`title`: AdmissionPolicy
- <a id="s-e714fd9d0a58"></a>`description`: One bounded, exact all-of classification admission rule.
- <a id="s-05470c8cac7a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4825bdded5eb"></a>`automatic_preview` | no | type="string"; const="accept-ready" |  |
| <a id="s-fe0784373ca3"></a>`effective_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-7ca1adfe1d63"></a>`format` | no | type="string"; const="stove0-admission-policy/v1" |  |
| <a id="s-f6d577c8df2c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fff97fd387e1"></a>`recipe_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-f81203e6998b"></a>`recipe_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-59bbce29c664"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a53f69ac6c67"></a>`required_tags` | yes | type="array"; minItems=1; maxItems=100; items=(#/components/schemas/CollectionTag); additional keys=`x-riverhog-extent` |  |
| <a id="s-558e3f83fb20"></a>`revision` | yes | type="integer"; minimum=1 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-fe0784373ca3) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field recipe_id](#s-fff97fd387e1) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field recipe_sha256](#s-59bbce29c664) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field required_tags](#s-a53f69ac6c67) | `cardinality · items · contract_max` | maximum=100; minimum=1; reason="bounded-exact-classification-admission-predicate" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-87adb5017475"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-3f7343cdbff5"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-f4a3a69f7b7c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPolicy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b922dd931b907481cef3ecf609f8bc3f55ee33cb8bf1736c53e552822b5a43b5 -->

```json
{
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
        "$ref": "#/components/schemas/CollectionTag"
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
}
```
