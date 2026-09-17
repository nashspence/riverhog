# schemas: TargetInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetinapplicable:9a19a7aba9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-804f4118a3"></a>

- <a id="s-9c2f9fcf38"></a>`type`: `"object"`
- <a id="s-34d4e3e250"></a>`additionalProperties`: `false`
- <a id="s-58b4939248"></a>`required`: `["code","message"]`
- <a id="s-01a661a990"></a>`title`: `"TargetInapplicable"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b21f85ed85"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-c4a2407994"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1000; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field message](#s-c4a2407994) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-6449f9cf6f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-2af02bdd13"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetInapplicable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6e7b7f0a6d88757ed930e374dc9c6877fd96d32d1a689c2d8fbc8e6a3838457 -->

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
    }
  },
  "required": [
    "code",
    "message"
  ],
  "title": "TargetInapplicable",
  "type": "object"
}
```

</details>
