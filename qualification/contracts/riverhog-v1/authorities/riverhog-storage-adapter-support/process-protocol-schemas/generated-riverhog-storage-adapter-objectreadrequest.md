# generated:riverhog-storage-adapter: ObjectReadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectreadrequest:713962a39d -->

Exact externally visible contract owned by this semantic dossier.

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
| <a id="s-debcf1b15e"></a>`expected_bytes` | yes | type="integer"; minimum=0; title="Expected Bytes" |  |
| <a id="s-6d80e36046"></a>`object` | yes | [ObjectLocator](#s-a35c176f55) |  |
| <a id="s-57ba00ac3d"></a>`offset` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null; title="Offset" |  |
| <a id="s-56c4609936"></a>`size` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null; title="Size" |  |

### Definitions

- [ObjectLocator](#s-a35c176f55)

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

- <a id="pa-fe5ac9f6d4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectReadRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f080cf57778767bde220ff2111c9ba3501ad753c8492b1759db04b8451d108d -->

```json
{
  "$defs": {
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
      "minimum": 0,
      "title": "Expected Bytes",
      "type": "integer"
    },
    "object": {
      "$ref": "#/$defs/ObjectLocator"
    },
    "offset": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Offset"
    },
    "size": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Size"
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
