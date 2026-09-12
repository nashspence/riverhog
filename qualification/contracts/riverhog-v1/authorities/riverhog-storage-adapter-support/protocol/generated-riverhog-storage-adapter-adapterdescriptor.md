# generated:riverhog-storage-adapter: AdapterDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-adapterdescriptor:fd5ddc3b83 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-4d4503852a"></a>
- <a id="s-c33981da12"></a>`title`: AdapterDescriptor
- <a id="s-eca284d012"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6157396c02"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ad077bc43a"></a>`implementation_version` | yes | type="string"; minLength=1; maxLength=120 |  |
| <a id="s-e4c53eefe8"></a>`maximum_segment_bytes` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-bca1f9556e"></a>`maximum_segment_count` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-7886488551"></a>`minimum_nonfinal_segment_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-f345896a9f"></a>`protocol` | no | type="string"; const="riverhog-storage-adapter/v1" |  |
| <a id="s-33e9ebd76a"></a>`read_mode` | yes | type="string"; enum=["immediate","restore_required"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=120; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field implementation_version](#s-ad077bc43a) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-3894baf76c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-bbc4c25d34"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/AdapterDescriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cee17550dd897297e60b7829813a6b432932a360b38b800c81292b1cbdda754 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "implementation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Implementation Id",
      "type": "string"
    },
    "implementation_version": {
      "maxLength": 120,
      "minLength": 1,
      "title": "Implementation Version",
      "type": "string"
    },
    "maximum_segment_bytes": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Maximum Segment Bytes"
    },
    "maximum_segment_count": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Maximum Segment Count"
    },
    "minimum_nonfinal_segment_bytes": {
      "minimum": 1,
      "title": "Minimum Nonfinal Segment Bytes",
      "type": "integer"
    },
    "protocol": {
      "const": "riverhog-storage-adapter/v1",
      "default": "riverhog-storage-adapter/v1",
      "title": "Protocol",
      "type": "string"
    },
    "read_mode": {
      "enum": [
        "immediate",
        "restore_required"
      ],
      "title": "Read Mode",
      "type": "string"
    }
  },
  "required": [
    "implementation_id",
    "implementation_version",
    "read_mode",
    "minimum_nonfinal_segment_bytes"
  ],
  "title": "AdapterDescriptor",
  "type": "object"
}
```
