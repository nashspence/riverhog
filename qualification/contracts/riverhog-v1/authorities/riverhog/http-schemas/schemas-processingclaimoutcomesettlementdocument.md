# schemas: ProcessingClaimOutcomeSettlementDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimoutcomesettlementdocument:2940173f94 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b7a81be779"></a>

- <a id="s-4aff1b04cd"></a>`type`: `"object"`
- <a id="s-239b3010d8"></a>`additionalProperties`: `false`
- `if`: [See `if`](#s-7bb79833a6)
- <a id="s-d7ded02331"></a>`required`: `["outcomes","retirement_policy","retirement_grace_seconds"]`
- `then`: [See `then`](#s-e46edf88b2)
- <a id="s-86326ca924"></a>`title`: `"ProcessingClaimOutcomeSettlementDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66e7037bf1"></a>`outcomes` | yes | [ExactSetIdentityDocument](schemas-exactsetidentitydocument.md) |  |
| <a id="s-c9a1869ba6"></a>`retirement_grace_seconds` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |
| <a id="s-ca68cd4ae2"></a>`retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"]; title="Retirement Policy" |  |

### <a id="s-7bb79833a6"></a>`if`


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-022b350df7"></a>`retirement_policy` | no | const="retain" |  |

### <a id="s-e46edf88b2"></a>`then`


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8157a34385"></a>`retirement_grace_seconds` | no | const="0" |  |

## Maintained corroboration

### Referenced contract elements

- [ExactSetIdentityDocument](schemas-exactsetidentitydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

- <a id="pa-f06f4d1a60"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimOutcomeSettlementDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f56d4ee89333c41442d1f3c35a5dad06f2ccd0e7636cbe4c5d3f39518dab7671 -->

```json
{
  "additionalProperties": false,
  "if": {
    "properties": {
      "retirement_policy": {
        "const": "retain"
      }
    }
  },
  "properties": {
    "outcomes": {
      "$ref": "#/components/schemas/ExactSetIdentityDocument"
    },
    "retirement_grace_seconds": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    },
    "retirement_policy": {
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Retirement Policy",
      "type": "string"
    }
  },
  "required": [
    "outcomes",
    "retirement_policy",
    "retirement_grace_seconds"
  ],
  "then": {
    "properties": {
      "retirement_grace_seconds": {
        "const": "0"
      }
    }
  },
  "title": "ProcessingClaimOutcomeSettlementDocument",
  "type": "object"
}
```

</details>
