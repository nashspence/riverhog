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
- <a id="s-7d83a5b1b7"></a>`required`: `["principal_id"]`
- <a id="s-b7b55d1149"></a>`title`: `"RiverhogActor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dc7ca6a622"></a>`key_id` | no | anyOf=[(type="string"; maxLength=300; minLength=1); (type="null")]; title="Key Id" |  |
| <a id="s-dedbb66501"></a>`principal_id` | yes | [PrincipalId](schemas-principalid.md); maxLength=160 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-200905861e"></a>[field key_id · string value](#s-dc7ca6a622) | `length · characters · contract_max` | maximum=300; minimum=1 |
| [field principal_id](#s-dedbb66501) | `length · characters · contract_max` | maximum=160 |

## Maintained corroboration

### Referenced contract elements

- [PrincipalId](schemas-principalid.md)

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
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogActor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2bb91dce20d755a785a7b4e877b25fb0f5ee4263ed42eaf195eb72916f89eb6 -->

```json
{
  "additionalProperties": false,
  "properties": {
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
    },
    "principal_id": {
      "$ref": "#/components/schemas/PrincipalId",
      "maxLength": 160
    }
  },
  "required": [
    "principal_id"
  ],
  "title": "RiverhogActor",
  "type": "object"
}
```

</details>
