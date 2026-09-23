# schemas: WorkClaimView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workclaimview:60c2049c64 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-9ac59618b0"></a>

- <a id="s-c03de4aca4"></a>`type`: `"object"`
- <a id="s-fa587d873e"></a>`additionalProperties`: `false`
- <a id="s-00729d23fa"></a>`required`: `["claim_id","fence"]`
- <a id="s-a28875c808"></a>`title`: `"WorkClaimView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c299af7455"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-eaac47bfb4"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=160; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-c299af7455) | `length · characters · contract_max` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b919c0b753"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-6e2e04c33e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkClaimView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5836e31fafa2f07e9c5cb14ccd4562b4c1e719d136d58b41e3f193e6c594bc53 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Claim Id",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    }
  },
  "required": [
    "claim_id",
    "fence"
  ],
  "title": "WorkClaimView",
  "type": "object"
}
```

</details>
