# schemas: ProcessingClaimOutcomesSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimoutcomessettledocument:2844c60457 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7ba16e9301"></a>

- <a id="s-be047dc069"></a>`type`: `"object"`
- <a id="s-39d310e232"></a>`additionalProperties`: `false`
- `if`: [See `if`](#s-097084343c)
- <a id="s-6ce96fb578"></a>`required`: `["fence"]`
- `then`: [See `then`](#s-e69183022b)
- <a id="s-0275659017"></a>`title`: `"ProcessingClaimOutcomesSettleDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f42f23efd4"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-513af7c9a5"></a>`source_collection_retirement_grace_seconds` | no | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |
| <a id="s-cc980295cc"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |

### <a id="s-097084343c"></a>`if`


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-23835994ec"></a>`source_collection_retirement_policy` | no | const="retain" |  |

### <a id="s-e69183022b"></a>`then`


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-48aa47e4d8"></a>`source_collection_retirement_grace_seconds` | no | const="0" |  |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

- <a id="pa-102797ab65"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimOutcomesSettleDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14a1daebeaf33b5d6d254ea00db1bb44fccd94b07339d7ad6103071cffeeaf0e -->

```json
{
  "additionalProperties": false,
  "if": {
    "properties": {
      "source_collection_retirement_policy": {
        "const": "retain"
      }
    }
  },
  "properties": {
    "fence": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "source_collection_retirement_grace_seconds": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    },
    "source_collection_retirement_policy": {
      "default": "retain",
      "description": "Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks.",
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Source Collection Retirement Policy",
      "type": "string"
    }
  },
  "required": [
    "fence"
  ],
  "then": {
    "properties": {
      "source_collection_retirement_grace_seconds": {
        "const": "0"
      }
    }
  },
  "title": "ProcessingClaimOutcomesSettleDocument",
  "type": "object"
}
```

</details>
