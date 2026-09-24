# schemas: DepartureEffectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-departureeffectreceipt:db4f36e999 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-64870ae2d8"></a>

- <a id="s-2c01bb8ce5"></a>`type`: `"object"`
- <a id="s-27e26d39bd"></a>`additionalProperties`: `false`
- <a id="s-ccedfdf248"></a>`required`: `["departure_id","target_identity","result","receipt_sha256"]`
- <a id="s-ea76481ebe"></a>`title`: `"DepartureEffectReceipt"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ee33d503d"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Departure Id" |  |
| <a id="s-7a8ba1ee6b"></a>`format` | no | type="string"; const="stove0-departure-effect-receipt/v1"; default="stove0-departure-effect-receipt/v1"; title="Format" |  |
| <a id="s-79fe7a2614"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Receipt Sha256" |  |
| <a id="s-46bdf44a30"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Result"; x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-departure-effect-receipt"} |  |
| <a id="s-7c9d797e64"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Identity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field result](#s-46bdf44a30) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field departure_id](#s-9ee33d503d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field receipt_sha256](#s-79fe7a2614) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field result](#s-46bdf44a30) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-departure-effect-receipt"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field target_identity](#s-7c9d797e64) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [JsonValue](schemas-jsonvalue.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b287fcd98d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-9460593590"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-56fadf1fc3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/DepartureEffectReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09c5f904492afaf9cc50fd5cc28fc1c2ee8bba4f1422896d6b630e21112b6558 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "departure_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Departure Id",
      "type": "string"
    },
    "format": {
      "const": "stove0-departure-effect-receipt/v1",
      "default": "stove0-departure-effect-receipt/v1",
      "title": "Format",
      "type": "string"
    },
    "receipt_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Receipt Sha256",
      "type": "string"
    },
    "result": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Result",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 65536,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-departure-effect-receipt"
      }
    },
    "target_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Identity",
      "type": "string"
    }
  },
  "required": [
    "departure_id",
    "target_identity",
    "result",
    "receipt_sha256"
  ],
  "title": "DepartureEffectReceipt",
  "type": "object"
}
```

</details>
