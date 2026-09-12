# schemas: AdmissionIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionintent:ca5ce1169b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-ba0530c236"></a>
- <a id="s-0aec38de14"></a>`title`: AdmissionIntent
- <a id="s-50460f4d51"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-edd0a3adb5"></a>`admission_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0bad523de4"></a>`collection` | yes | #/components/schemas/CatalogSyncDescriptor |  |
| <a id="s-4b7ce35bca"></a>`effective_intent` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-ec404034ae"></a>`format` | no | type="string"; const="stove0-admission-intent/v1" |  |
| <a id="s-33c67405d9"></a>`policy_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-f4f9002e65"></a>`policy_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-75bb897925"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4ff09b93ec"></a>`recipe_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-b4e40a831d"></a>`recipe_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-17ec6189a3"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-81dfbfb6bf"></a>`required_tags` | yes | type="array"; items=(#/components/schemas/CollectionTag) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-4b7ce35bca) | `cardinality · entries · operational_policy` | shared above |
| [field required_tags](#s-81dfbfb6bf) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field admission_id](#s-edd0a3adb5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field policy_id](#s-33c67405d9) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field policy_sha256](#s-75bb897925) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field recipe_id](#s-4ff09b93ec) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field recipe_sha256](#s-17ec6189a3) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md)
- [schemas: CollectionTag](schemas-collectiontag.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-874407754c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b12d644e27"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-e857dea921"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0944c132ed593050435d503c92c7a9b57d07fea52ef8280ed7641f12f0c1f12f -->

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
