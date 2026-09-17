# schemas: WorkFailureView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workfailureview:d79b5a1989 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b4761108c0"></a>

- <a id="s-e20222ca3a"></a>`type`: `"object"`
- <a id="s-7c798eb9a5"></a>`additionalProperties`: `false`
- <a id="s-e5849b5a6e"></a>`required`: `["code","message","retryable"]`
- <a id="s-80e79fb104"></a>`title`: `"WorkFailureView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7402188896"></a>`code` | yes | type="string"; maxLength=160; minLength=1; title="Code" |  |
| <a id="s-c750f10414"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-ee8f4b742c"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field code](#s-7402188896) | `length · characters · contract_max` | maximum=160 |
| [field message](#s-c750f10414) | `length · characters · contract_max` | maximum=1000 |

## Governing policies

- <a id="pa-9a12e5b5d9"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-f58d512ed1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkFailureView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a61374424ae470a2c694ac13502faf98f91af05992638c02e1d5b5380dac0c4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Code",
      "type": "string"
    },
    "message": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Message",
      "type": "string"
    },
    "retryable": {
      "title": "Retryable",
      "type": "boolean"
    }
  },
  "required": [
    "code",
    "message",
    "retryable"
  ],
  "title": "WorkFailureView",
  "type": "object"
}
```

</details>
