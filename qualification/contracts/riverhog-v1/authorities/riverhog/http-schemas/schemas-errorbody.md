# schemas: ErrorBody

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-errorbody:afec5f9e1c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e341a8fff0"></a>

- <a id="s-c42ede3b72"></a>`type`: `"object"`
- <a id="s-b5026c3cb1"></a>`additionalProperties`: `false`
- <a id="s-625a1c18a9"></a>`required`: `["code","message"]`
- <a id="s-d16bb1c543"></a>`title`: `"ErrorBody"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6ed442e30b"></a>`code` | yes | type="string"; minLength=1; title="Code" |  |
| <a id="s-6cf5a8898b"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Details" |  |
| <a id="s-08f76da0e6"></a>`message` | yes | type="string"; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b9dfcb3616"></a>[field details · object value](#s-6cf5a8898b) | `cardinality · entries · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-905bf03d33"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c4b7bd2af6"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ErrorBody`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6760c34bd592b6639dc349282b6e02f6cebfa1985aa1a5b2c1943c0faa367741 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "minLength": 1,
      "title": "Code",
      "type": "string"
    },
    "details": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Details"
    },
    "message": {
      "minLength": 1,
      "title": "Message",
      "type": "string"
    }
  },
  "required": [
    "code",
    "message"
  ],
  "title": "ErrorBody",
  "type": "object"
}
```

</details>
