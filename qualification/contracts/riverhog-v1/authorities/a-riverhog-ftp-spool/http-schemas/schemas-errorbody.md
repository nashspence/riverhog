# schemas: ErrorBody

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-errorbody:1bc53365db -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e5ac430859"></a>

- <a id="s-afd293c18a"></a>`type`: `"object"`
- <a id="s-2d4ce478a1"></a>`additionalProperties`: `false`
- <a id="s-aed9aecad5"></a>`required`: `["code","message"]`
- <a id="s-fdf544d70e"></a>`title`: `"ErrorBody"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0585801de1"></a>`code` | yes | type="string"; minLength=1; title="Code" |  |
| <a id="s-c524b67053"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null; title="Details" |  |
| <a id="s-72f507a34d"></a>`message` | yes | type="string"; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-ftp-spool"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-750d294ce8"></a>[field details · object value](#s-c524b67053) | `cardinality · entries · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-12ba80ad58"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-24ec92c55f"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/ErrorBody`

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
