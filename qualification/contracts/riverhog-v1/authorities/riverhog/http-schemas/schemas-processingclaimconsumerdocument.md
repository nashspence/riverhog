# schemas: ProcessingClaimConsumerDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimconsumerdocument:0b36b64aae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c9eaaa92fe"></a>

- <a id="s-27e1e6899e"></a>`type`: `"object"`
- <a id="s-4b23246723"></a>`additionalProperties`: `false`
- <a id="s-568f7b2a1b"></a>`required`: `["app"]`
- <a id="s-a4f4a7835e"></a>`title`: `"ProcessingClaimConsumerDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e3e65ac8a5"></a>`app` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="App" |  |
| <a id="s-560dbaafaf"></a>`key_id` | no | anyOf=[(type="string"; maxLength=300; minLength=1); (type="null")]; title="Key Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=300; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-16ba0f0f0e"></a>[field key_id · string value](#s-560dbaafaf) | `length · characters · contract_max` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-77c286d2a4"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-486eeca936"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimConsumerDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7cd765d2f05f41f51d27802da1d22e63f901cd257453447ccce3a8705be74d9b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "app": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "App",
      "type": "string"
    },
    "key_id": {
      "anyOf": [
        {
          "maxLength": 300,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Key Id"
    }
  },
  "required": [
    "app"
  ],
  "title": "ProcessingClaimConsumerDocument",
  "type": "object"
}
```

</details>
