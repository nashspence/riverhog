# generated:riverhog-storage-adapter: WriteSegmentRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-304d75949a:8da143e88e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9929dcbde1"></a>
- <a id="s-c3edda53c8"></a>`title`: WriteSegmentRequest
- <a id="s-ae1b9f4813"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce0948c417"></a>`number` | yes | type="integer"; minimum=1 |  |
| <a id="s-493606ef17"></a>`session` | yes | #/$defs/WriteSession |  |
| <a id="s-2bda4ac939"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-dcf667e799"></a>`WriteSession` | type="object"; fields=`expected_bytes`, `object_path`, `write_token`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-b287dd0552"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e51e90e06c0f6a6ca83920b56cbb0696ced3950650ea8d743c33191cb2cf7d31 -->

```json
{
  "$defs": {
    "WriteSession": {
      "additionalProperties": false,
      "properties": {
        "expected_bytes": {
          "description": "Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal.",
          "minimum": 1,
          "title": "Expected Bytes",
          "type": "integer"
        },
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Object Path",
          "type": "string"
        },
        "write_token": {
          "description": "Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal.",
          "maxLength": 4000,
          "minLength": 1,
          "title": "Write Token",
          "type": "string"
        }
      },
      "required": [
        "object_path",
        "expected_bytes",
        "write_token"
      ],
      "title": "WriteSession",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "number": {
      "minimum": 1,
      "title": "Number",
      "type": "integer"
    },
    "session": {
      "$ref": "#/$defs/WriteSession"
    },
    "stored_bytes": {
      "minimum": 1,
      "title": "Stored Bytes",
      "type": "integer"
    }
  },
  "required": [
    "session",
    "number",
    "stored_bytes"
  ],
  "title": "WriteSegmentRequest",
  "type": "object"
}
```
