# schemas: WorkflowPreviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workflowpreviewin:190c432da1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-91b3bb9a57"></a>

- <a id="s-3a1816b961"></a>`type`: `"object"`
- <a id="s-b4e2d53cae"></a>`additionalProperties`: `false`
- <a id="s-f2c853f3bf"></a>`required`: `["recipe_id","inputs"]`
- <a id="s-c39e031afe"></a>`title`: `"WorkflowPreviewIn"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8250b31586"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Effective Intent" |  |
| <a id="s-6c92b782aa"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](schemas-collectionrootref.md)); minItems=1; title="Inputs" |  |
| <a id="s-5308fa97b6"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-231030b92d"></a>`recipe_revision` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; title="Recipe Revision" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-8250b31586) | `cardinality · entries · operational_policy` | shared above |
| [field inputs](#s-6c92b782aa) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=160; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field recipe_id](#s-5308fa97b6) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [CollectionRootRef](schemas-collectionrootref.md)
- [JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-a9f8a21551"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-1e2526838b"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-001b60fae4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPreviewIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7a96d936032649ccfe393754cd7845b9f8251a631eef1e7af249617d68aecea -->

```json
{
  "additionalProperties": false,
  "properties": {
    "effective_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Effective Intent",
      "type": "object"
    },
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/CollectionRootRef"
      },
      "minItems": 1,
      "title": "Inputs",
      "type": "array"
    },
    "recipe_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Recipe Id",
      "type": "string"
    },
    "recipe_revision": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Recipe Revision"
    }
  },
  "required": [
    "recipe_id",
    "inputs"
  ],
  "title": "WorkflowPreviewIn",
  "type": "object"
}
```

</details>
