# schemas: ContentObservationFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-contentobservationfailure:34f557fae3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d018487697"></a>

- <a id="s-50679c7675"></a>`type`: `"object"`
- <a id="s-6fa6eb2746"></a>`additionalProperties`: `false`
- <a id="s-3b703fc095"></a>`required`: `["code","message","retryable"]`
- <a id="s-402898a336"></a>`title`: `"ContentObservationFailure"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2e91d67aed"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-cb4dd637cb"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-dff1933dbb"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1000; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field message](#s-cb4dd637cb) | `length · characters · contract_max` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6876436f50"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4aca00685b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ContentObservationFailure`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 889c1430b67b01adfb0b7bdd9899905b2edc8eff14a99d28b2c7446cde24f2c9 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
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
  "title": "ContentObservationFailure",
  "type": "object"
}
```

</details>
