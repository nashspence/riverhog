# generated:riverhog-storage-adapter: WriteSegmentReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-6ba9873e65:190994f675 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-2da953d573"></a>

- <a id="s-85cd4b164c"></a>`type`: `"object"`
- <a id="s-e3c51c7f73"></a>`additionalProperties`: `false`
- <a id="s-91387670fb"></a>`required`: `["number","segment_token","stored_bytes"]`
- <a id="s-aa75a865b1"></a>`title`: `"WriteSegmentReceipt"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88035ebc6e"></a>`number` | yes | type="integer"; minimum=1; title="Number" |  |
| <a id="s-0f6d9c2a15"></a>`segment_token` | yes | type="string"; maxLength=4000; minLength=1; title="Segment Token" |  |
| <a id="s-4186773e61"></a>`stored_bytes` | yes | type="integer"; minimum=1; title="Stored Bytes" |  |
| <a id="s-48aa910874"></a>`stored_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Stored Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field segment_token](#s-0f6d9c2a15) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| <a id="s-dae312493c"></a>[field stored_sha256 · string value](#s-48aa910874) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2f058984c4"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-9c41f1a694"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 78f82d84ff68316a37a5d4d95d212d082ea0eada74506b6745aee67978f169da -->

```json
{
  "additionalProperties": false,
  "properties": {
    "number": {
      "minimum": 1,
      "title": "Number",
      "type": "integer"
    },
    "segment_token": {
      "maxLength": 4000,
      "minLength": 1,
      "title": "Segment Token",
      "type": "string"
    },
    "stored_bytes": {
      "minimum": 1,
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stored_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Stored Sha256"
    }
  },
  "required": [
    "number",
    "segment_token",
    "stored_bytes"
  ],
  "title": "WriteSegmentReceipt",
  "type": "object"
}
```

</details>
