# schemas: RiverhogActor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-riverhogactor:736b1a4e6c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-dfa081e99d"></a>

- <a id="s-3e08a408c6"></a>`type`: `"object"`
- <a id="s-0b79164001"></a>`additionalProperties`: `false`
- <a id="s-7d83a5b1b7"></a>`required`: `["app"]`
- <a id="s-b7b55d1149"></a>`title`: `"RiverhogActor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d49c0fa88"></a>`app` | yes | type="string"; maxLength=160; minLength=1; title="App" |  |
| <a id="s-dc7ca6a622"></a>`key_id` | no | anyOf=[(type="string"; maxLength=300; minLength=1); (type="null")]; title="Key Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field app](#s-3d49c0fa88) | `length · characters · contract_max` | maximum=160 |
| <a id="s-200905861e"></a>[field key_id · string value](#s-dc7ca6a622) | `length · characters · contract_max` | maximum=300 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b1c389a314"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e2b5fe54f9"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogActor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae88be9c9fa5a2e60977b689011476674a5c673474d5e841bf78ebb369101905 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "app": {
      "maxLength": 160,
      "minLength": 1,
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
  "title": "RiverhogActor",
  "type": "object"
}
```

</details>
