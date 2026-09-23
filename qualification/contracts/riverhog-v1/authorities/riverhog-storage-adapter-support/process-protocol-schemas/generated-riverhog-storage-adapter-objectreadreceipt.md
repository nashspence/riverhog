# generated:riverhog-storage-adapter: ObjectReadReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectreadreceipt:4b4f18b1a7 -->

Adapter-observed identity and range for one single-pass read.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-af23daf6d7"></a>

- <a id="s-fd118908e7"></a>`type`: `"object"`
- <a id="s-149f776e44"></a>`additionalProperties`: `false`
- <a id="s-0eb9c3d724"></a>`description`: `"Adapter-observed identity and range for one single-pass read."`
- <a id="s-1af766c56e"></a>`required`: `["object","total_bytes","offset","read_bytes"]`
- <a id="s-e97b07454c"></a>`title`: `"ObjectReadReceipt"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-036b19f39b"></a>`object` | yes | [ObjectLocator](#s-43c741ab8f) |  |
| <a id="s-66b82b7e4f"></a>`offset` | yes | [NonnegativeDecimal](#s-04ccbcd3c8) |  |
| <a id="s-cbc2c869b9"></a>`read_bytes` | yes | [NonnegativeDecimal](#s-04ccbcd3c8) |  |
| <a id="s-214b4e2d30"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-04ccbcd3c8) |  |

### Definitions

- [NonnegativeDecimal](#s-04ccbcd3c8)
- [ObjectLocator](#s-43c741ab8f)

### <a id="s-04ccbcd3c8"></a>definition `NonnegativeDecimal`

- <a id="s-a4f2ac2bc4"></a>`type`: `"string"`
- <a id="s-057b6a6cd3"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-43c741ab8f"></a>definition `ObjectLocator`

- <a id="s-4242a7a160"></a>`type`: `"object"`
- <a id="s-52736fa855"></a>`additionalProperties`: `false`
- <a id="s-f130376dfa"></a>`required`: `["object_path"]`
- <a id="s-460c43e497"></a>`title`: `"ObjectLocator"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bb4835dae2"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-d0c342614f"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null; title="Revision" |  |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-5c2c31854c"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectReadReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 250cd17c37e3fc7dd7f3138baaa8404b1d8d4a26b504abbb217a4f37045d1715 -->

```json
{
  "$defs": {
    "NonnegativeDecimal": {
      "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
      "type": "string"
    },
    "ObjectLocator": {
      "additionalProperties": false,
      "properties": {
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Object Path",
          "type": "string"
        },
        "revision": {
          "anyOf": [
            {
              "maxLength": 2000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Revision"
        }
      },
      "required": [
        "object_path"
      ],
      "title": "ObjectLocator",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Adapter-observed identity and range for one single-pass read.",
  "properties": {
    "object": {
      "$ref": "#/$defs/ObjectLocator"
    },
    "offset": {
      "$ref": "#/$defs/NonnegativeDecimal"
    },
    "read_bytes": {
      "$ref": "#/$defs/NonnegativeDecimal"
    },
    "total_bytes": {
      "$ref": "#/$defs/NonnegativeDecimal"
    }
  },
  "required": [
    "object",
    "total_bytes",
    "offset",
    "read_bytes"
  ],
  "title": "ObjectReadReceipt",
  "type": "object"
}
```

</details>
