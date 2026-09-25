# generated:riverhog-storage-adapter: AdapterDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-adapterdescriptor:e309d8ef07 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-4d4503852a"></a>

- <a id="s-eca284d012"></a>`type`: `"object"`
- <a id="s-808a2f7150"></a>`additionalProperties`: `false`
- <a id="s-59e897ad89"></a>`required`: `["storage_incarnation_id","implementation_id","implementation_version","read_mode","minimum_nonfinal_segment_bytes"]`
- <a id="s-c33981da12"></a>`title`: `"AdapterDescriptor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6157396c02"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-ad077bc43a"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-e4c53eefe8"></a>`maximum_segment_bytes` | no | anyOf=[([PositiveDecimal](#s-dd88e4b546)); (type="null")]; default=null |  |
| <a id="s-bca1f9556e"></a>`maximum_segment_count` | no | anyOf=[([PositiveDecimal](#s-dd88e4b546)); (type="null")]; default=null |  |
| <a id="s-7886488551"></a>`minimum_nonfinal_segment_bytes` | yes | [PositiveDecimal](#s-dd88e4b546) |  |
| <a id="s-f345896a9f"></a>`protocol` | no | type="string"; const="riverhog-storage-adapter/v1"; default="riverhog-storage-adapter/v1"; title="Protocol" |  |
| <a id="s-33e9ebd76a"></a>`read_mode` | yes | type="string"; enum=["immediate","restore_required"]; title="Read Mode" |  |
| <a id="s-b3fb003a0d"></a>`storage_incarnation_id` | yes | type="string"; pattern="^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"; title="Storage Incarnation Id" |  |

### Definitions

- [PositiveDecimal](#s-dd88e4b546)

### <a id="s-dd88e4b546"></a>definition `PositiveDecimal`

- <a id="s-8193162af1"></a>`type`: `"string"`
- <a id="s-59d3d30f66"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=120; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field implementation_version](#s-ad077bc43a) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-7e392f0c92"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-c70497d24e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/AdapterDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f3adb99db4c16193a0f47afd71dbf859e35137515f3e6248d39ca31af85bcdf5 -->

```json
{
  "$defs": {
    "PositiveDecimal": {
      "pattern": "^[1-9][0-9]*(?![\\s\\S])",
      "type": "string"
    }
  },
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
          "$ref": "#/$defs/PositiveDecimal"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "maximum_segment_count": {
      "anyOf": [
        {
          "$ref": "#/$defs/PositiveDecimal"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "minimum_nonfinal_segment_bytes": {
      "$ref": "#/$defs/PositiveDecimal"
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
    },
    "storage_incarnation_id": {
      "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
      "title": "Storage Incarnation Id",
      "type": "string"
    }
  },
  "required": [
    "storage_incarnation_id",
    "implementation_id",
    "implementation_version",
    "read_mode",
    "minimum_nonfinal_segment_bytes"
  ],
  "title": "AdapterDescriptor",
  "type": "object"
}
```

</details>
