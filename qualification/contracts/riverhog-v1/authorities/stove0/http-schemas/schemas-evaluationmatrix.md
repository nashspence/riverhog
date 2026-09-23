# schemas: EvaluationMatrix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationmatrix:f446f0735c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-42674da55c"></a>

- <a id="s-b2fcf12d3e"></a>`type`: `"object"`
- <a id="s-0277a9d53b"></a>`additionalProperties`: `false`
- <a id="s-9556aeae46"></a>`required`: `["variants","matrix_sha256"]`
- <a id="s-4bc22de259"></a>`title`: `"EvaluationMatrix"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-36c2f17f34"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1"; default="stove0-evaluation-matrix/v1"; title="Format" |  |
| <a id="s-184e486a2e"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Matrix Sha256" |  |
| <a id="s-4177bbefab"></a>`variants` | yes | type="array"; items=([EvaluationVariant](schemas-evaluationvariant.md)); minItems=1; title="Variants" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field variants](#s-4177bbefab) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field matrix_sha256](#s-184e486a2e) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [EvaluationVariant](schemas-evaluationvariant.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b15c268758"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-70499cba06"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-8b4f89562e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationMatrix`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2ca6c94739f3ab55f1dd9a80c12df7af452e1bab0cff3d020cdc543918b73e9 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-evaluation-matrix/v1",
      "default": "stove0-evaluation-matrix/v1",
      "title": "Format",
      "type": "string"
    },
    "matrix_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Matrix Sha256",
      "type": "string"
    },
    "variants": {
      "items": {
        "$ref": "#/components/schemas/EvaluationVariant"
      },
      "minItems": 1,
      "title": "Variants",
      "type": "array"
    }
  },
  "required": [
    "variants",
    "matrix_sha256"
  ],
  "title": "EvaluationMatrix",
  "type": "object"
}
```

</details>
