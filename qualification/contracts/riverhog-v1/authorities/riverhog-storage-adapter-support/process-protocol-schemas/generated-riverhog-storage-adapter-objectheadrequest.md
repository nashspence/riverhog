# generated:riverhog-storage-adapter: ObjectHeadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectheadrequest:427abbae4c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-1d0923d679"></a>

- <a id="s-9c07a6f0f2"></a>`type`: `"object"`
- <a id="s-85883b5aa8"></a>`additionalProperties`: `false`
- <a id="s-4f333ef0a1"></a>`required`: `["object","expected_placement"]`
- <a id="s-43f6a60384"></a>`title`: `"ObjectHeadRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6e4b7e80f0"></a>`expected_placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-5415a60fb9"></a>`object` | yes | [ObjectLocator](#s-3721011c38) |  |

### Definitions

- [ObjectLocator](#s-3721011c38)

### <a id="s-3721011c38"></a>definition `ObjectLocator`

- <a id="s-06f7981479"></a>`type`: `"object"`
- <a id="s-6f648c2376"></a>`additionalProperties`: `false`
- <a id="s-c56d093e77"></a>`required`: `["object_path"]`
- <a id="s-96205888f8"></a>`title`: `"ObjectLocator"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bedd7888d"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-0391dfaa3e"></a>`revision` | no | anyOf=(type="string"; maxLength=2000; minLength=1) \| (type="null"); default=null |  |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-0770b3e46b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectHeadRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e0283dd234d9ca0e0f4a026cfc1d8102dd40e8a31f86eb9e99e8613976200e5 -->

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
    "expected_placement": {
      "enum": [
        "archive",
        "immediate"
      ],
      "title": "Expected Placement",
      "type": "string"
    },
    "object": {
      "$ref": "#/$defs/ObjectLocator"
    }
  },
  "required": [
    "object",
    "expected_placement"
  ],
  "title": "ObjectHeadRequest",
  "type": "object"
}
```

</details>
