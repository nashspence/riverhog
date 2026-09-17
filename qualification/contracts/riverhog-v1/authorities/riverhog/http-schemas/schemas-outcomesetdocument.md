# schemas: OutcomeSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-outcomesetdocument:f956c8f3de -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8256570e62"></a>

- <a id="s-10cec658a2"></a>`type`: `"object"`
- <a id="s-08a0d1fcb9"></a>`additionalProperties`: `false`
- <a id="s-c29e31a699"></a>`required`: `["state","count"]`
- <a id="s-8f4891b07c"></a>`title`: `"OutcomeSetDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8b98c68813"></a>`authority` | no | anyOf=[([ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)); (type="null")] |  |
| <a id="s-c3431dbb24"></a>`count` | yes | type="integer"; minimum=0; title="Count" |  |
| <a id="s-102c3f6916"></a>`failure` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; title="Failure" |  |
| <a id="s-da0eeee06e"></a>`state` | yes | type="string"; enum=["receiving","sealing","sealed","failed"]; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field count](#s-c3431dbb24) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1000; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-a5afa51c28"></a>[field failure · string value](#s-102c3f6916) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Governing policies

- <a id="pa-d8752f353e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-aa2e2eb903"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-27e15b31ed"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OutcomeSetDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8576ac8304a0301e16f6f81bd65ae1e818011299bdb236ea4db30959d6365dc5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExactSetAuthorityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "count": {
      "minimum": 0,
      "title": "Count",
      "type": "integer"
    },
    "failure": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "state": {
      "enum": [
        "receiving",
        "sealing",
        "sealed",
        "failed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state",
    "count"
  ],
  "title": "OutcomeSetDocument",
  "type": "object"
}
```

</details>
