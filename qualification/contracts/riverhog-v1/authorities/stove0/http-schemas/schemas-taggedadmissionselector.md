# schemas: TaggedAdmissionSelector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-taggedadmissionselector:568187b8ae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-484d76db00"></a>

- <a id="s-2858b18151"></a>`type`: `"object"`
- <a id="s-4fcdb6b735"></a>`additionalProperties`: `false`
- <a id="s-a320e2791a"></a>`required`: `["required"]`
- <a id="s-b5aabec3be"></a>`title`: `"TaggedAdmissionSelector"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4bb8d9ae76"></a>`kind` | no | type="string"; const="tags"; default="tags"; title="Kind" |  |
| <a id="s-78019ec85d"></a>`required` | yes | type="array"; items=([CollectionTag](schemas-collectiontag.md)); maxItems=100; minItems=1; title="Required"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="bounded-exact-classification-admission-predicate"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field required](#s-78019ec85d) | `cardinality · items · contract_max` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionTag](schemas-collectiontag.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3d65735b63"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-06a4a96538"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TaggedAdmissionSelector`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f92bb93a5cef7282ac9a2018237b6c8d5a8d726668a76e5aff37c0726a5c8f29 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "kind": {
      "const": "tags",
      "default": "tags",
      "title": "Kind",
      "type": "string"
    },
    "required": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "minItems": 1,
      "title": "Required",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-exact-classification-admission-predicate"
      }
    }
  },
  "required": [
    "required"
  ],
  "title": "TaggedAdmissionSelector",
  "type": "object"
}
```

</details>
