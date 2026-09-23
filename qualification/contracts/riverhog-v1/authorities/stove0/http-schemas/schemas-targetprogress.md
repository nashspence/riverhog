# schemas: TargetProgress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetprogress:e91820db66 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8286fd2437"></a>

- <a id="s-34d45a8485"></a>`type`: `"object"`
- <a id="s-7d0513dd37"></a>`additionalProperties`: `false`
- <a id="s-a04ab81aba"></a>`required`: `["phase","completed"]`
- <a id="s-b3591114ae"></a>`title`: `"TargetProgress"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-76d78c2b9f"></a>`completed` | yes | type="integer"; minimum=0; title="Completed" |  |
| <a id="s-ded255a1ab"></a>`phase` | yes | type="string"; maxLength=120; minLength=1; title="Phase" |  |
| <a id="s-edfb3f4d44"></a>`total` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; title="Total" |  |
| <a id="s-5abc9e6e1c"></a>`unit` | no | anyOf=[(type="string"; maxLength=40; minLength=1); (type="null")]; title="Unit" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field phase](#s-ded255a1ab) | `length · characters · contract_max` | maximum=120 |
| <a id="s-df67201d67"></a>[field unit · string value](#s-5abc9e6e1c) | `length · characters · contract_max` | maximum=40 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6be9ac6a38"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-fb58e5ffb3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetProgress`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39df6fbc583523c116848af049b9e80abb672fa8af47887fcd5f7a791684498a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "completed": {
      "minimum": 0,
      "title": "Completed",
      "type": "integer"
    },
    "phase": {
      "maxLength": 120,
      "minLength": 1,
      "title": "Phase",
      "type": "string"
    },
    "total": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Total"
    },
    "unit": {
      "anyOf": [
        {
          "maxLength": 40,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Unit"
    }
  },
  "required": [
    "phase",
    "completed"
  ],
  "title": "TargetProgress",
  "type": "object"
}
```

</details>
