# schemas: ExternalEffectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-externaleffectreceipt:54719848bd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-16593928e7"></a>

- <a id="s-fc576a4deb"></a>`type`: `"object"`
- <a id="s-05116e9e9a"></a>`additionalProperties`: `false`
- <a id="s-76e8050283"></a>`required`: `["job_id","request_sha256","target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result","receipt_sha256"]`
- <a id="s-740cf6e4bf"></a>`title`: `"ExternalEffectReceipt"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d2aaff7b04"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-aae5192c57"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1"; title="Format" |  |
| <a id="s-e9bbf6f1ac"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-7b9d012ea6"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-de82bc172b"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-25cc4872ef"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Receipt Sha256" |  |
| <a id="s-b860ac21ac"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-fdcf8150cf"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Result"; x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-dd3b400161"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field result](#s-fdcf8150cf) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_sha256](#s-d2aaff7b04) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field job_id](#s-e9bbf6f1ac) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field operation_contract_sha256](#s-7b9d012ea6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field plan_sha256](#s-de82bc172b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field receipt_sha256](#s-25cc4872ef) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field request_sha256](#s-b860ac21ac) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field result](#s-fdcf8150cf) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-external-effect-receipt"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field target_descriptor_sha256](#s-dd3b400161) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [JsonValue](schemas-jsonvalue.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dd867f3d08"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-27eff5e66c"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-32e6a2382d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ExternalEffectReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f7e79f0f2ec4d740955220ddf1b2223b311d3bf7009a9bee9e5ee7069abcea2 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "execution_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Execution Sha256",
      "type": "string"
    },
    "format": {
      "const": "stove0-external-effect-receipt/v1",
      "default": "stove0-external-effect-receipt/v1",
      "title": "Format",
      "type": "string"
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "operation_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Operation Contract Sha256",
      "type": "string"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "receipt_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Receipt Sha256",
      "type": "string"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
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
        "reason": "bounded-external-effect-receipt"
      }
    },
    "target_descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Descriptor Sha256",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "request_sha256",
    "target_descriptor_sha256",
    "operation_contract_sha256",
    "plan_sha256",
    "execution_sha256",
    "result",
    "receipt_sha256"
  ],
  "title": "ExternalEffectReceipt",
  "type": "object"
}
```

</details>
