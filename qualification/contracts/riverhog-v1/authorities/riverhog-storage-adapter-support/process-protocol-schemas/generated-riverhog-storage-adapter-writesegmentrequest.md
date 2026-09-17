# generated:riverhog-storage-adapter: WriteSegmentRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-304d75949a:42ca9869f2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-9929dcbde1"></a>

- <a id="s-ae1b9f4813"></a>`type`: `"object"`
- <a id="s-de74089348"></a>`additionalProperties`: `false`
- <a id="s-c8eba87cd9"></a>`required`: `["session","number","stored_bytes"]`
- <a id="s-c3edda53c8"></a>`title`: `"WriteSegmentRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce0948c417"></a>`number` | yes | type="integer"; minimum=1; title="Number" |  |
| <a id="s-493606ef17"></a>`session` | yes | [WriteSession](#s-dcf667e799) |  |
| <a id="s-2bda4ac939"></a>`stored_bytes` | yes | type="integer"; minimum=1; title="Stored Bytes" |  |

### Definitions

- [WriteSession](#s-dcf667e799)

### <a id="s-dcf667e799"></a>definition `WriteSession`

- <a id="s-8759e0bc60"></a>`type`: `"object"`
- <a id="s-67322ca484"></a>`additionalProperties`: `false`
- <a id="s-817b5b11aa"></a>`required`: `["object_path","expected_bytes","write_token"]`
- <a id="s-201e1d2415"></a>`title`: `"WriteSession"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-79867a2dcf"></a>`expected_bytes` | yes | type="integer"; minimum=1; title="Expected Bytes" | Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal. |
| <a id="s-6f6eec603c"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-743f3b5ac0"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1; title="Write Token" | Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal. |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-04c5db961f"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
