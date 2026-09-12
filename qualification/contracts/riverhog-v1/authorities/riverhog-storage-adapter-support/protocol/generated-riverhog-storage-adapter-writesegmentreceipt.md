# generated:riverhog-storage-adapter: WriteSegmentReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-6ba9873e65:ff59f32b06 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-2da953d573"></a>
- <a id="s-aa75a865b1"></a>`title`: WriteSegmentReceipt
- <a id="s-85cd4b164c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88035ebc6e"></a>`number` | yes | type="integer"; minimum=1 |  |
| <a id="s-0f6d9c2a15"></a>`segment_token` | yes | type="string"; minLength=1; maxLength=4000 |  |
| <a id="s-4186773e61"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-48aa910874"></a>`stored_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field segment_token](#s-0f6d9c2a15) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| <a id="s-dae312493c"></a>[field stored_sha256 · string value](#s-48aa910874) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-301c8b8475"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-a457a51b30"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentReceipt`

### Exact owned JSON

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
