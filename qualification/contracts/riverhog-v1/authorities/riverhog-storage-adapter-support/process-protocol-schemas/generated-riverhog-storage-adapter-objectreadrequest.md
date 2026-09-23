# generated:riverhog-storage-adapter: ObjectReadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectreadrequest:713962a39d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-081aefc53c"></a>

- <a id="s-9ff82f8017"></a>`type`: `"object"`
- <a id="s-9c7759bc68"></a>`additionalProperties`: `false`
- <a id="s-a2c4ba4972"></a>`required`: `["object","expected_bytes"]`
- <a id="s-2ef1a56d86"></a>`title`: `"ObjectReadRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-debcf1b15e"></a>`expected_bytes` | yes | [NonnegativeDecimal](#s-7e0a34ce1f) |  |
| <a id="s-6d80e36046"></a>`object` | yes | [ObjectLocator](#s-a35c176f55) |  |
| <a id="s-57ba00ac3d"></a>`offset` | no | anyOf=[([NonnegativeDecimal](#s-7e0a34ce1f)); (type="null")]; default=null |  |
| <a id="s-56c4609936"></a>`size` | no | anyOf=[([NonnegativeDecimal](#s-7e0a34ce1f)); (type="null")]; default=null |  |

### Definitions

- [NonnegativeDecimal](#s-7e0a34ce1f)
- [ObjectLocator](#s-a35c176f55)

### <a id="s-7e0a34ce1f"></a>definition `NonnegativeDecimal`

- <a id="s-9673f50c44"></a>`type`: `"string"`
- <a id="s-302da974e8"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-a35c176f55"></a>definition `ObjectLocator`

- <a id="s-40ee573162"></a>`type`: `"object"`
- <a id="s-764452a881"></a>`additionalProperties`: `false`
- <a id="s-94b8b90e99"></a>`required`: `["object_path"]`
- <a id="s-578d80a927"></a>`title`: `"ObjectLocator"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4dfbf936c2"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-ca9b9d8e31"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null; title="Revision" |  |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-fe5ac9f6d4"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectReadRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40b0c2a8fb49466de3f72787caecdc5ce3da3f780ec89cdcf58c61a8ee84f95e -->

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
  "properties": {
    "expected_bytes": {
      "$ref": "#/$defs/NonnegativeDecimal"
    },
    "object": {
      "$ref": "#/$defs/ObjectLocator"
    },
    "offset": {
      "anyOf": [
        {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "size": {
      "anyOf": [
        {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    }
  },
  "required": [
    "object",
    "expected_bytes"
  ],
  "title": "ObjectReadRequest",
  "type": "object"
}
```

</details>
