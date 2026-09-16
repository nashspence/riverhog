# generated:riverhog-storage-adapter: WriteSession

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writesession:54ab7331ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-5d276c77f5"></a>

- <a id="s-514d7def0e"></a>`type`: `"object"`
- <a id="s-bee685210f"></a>`additionalProperties`: `false`
- <a id="s-80cb8c0bda"></a>`required`: `["object_path","expected_bytes","write_token"]`
- <a id="s-9a6acf1bee"></a>`title`: `"WriteSession"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b573b398f3"></a>`expected_bytes` | yes | type="integer"; minimum=1; title="Expected Bytes" | Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal. |
| <a id="s-f382e94026"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-bb12b9b006"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1; title="Write Token" | Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal. |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field object_path](#s-f382e94026) | `length · characters · contract_max` | maximum=4096 |
| [field write_token](#s-bb12b9b006) | `length · characters · contract_max` | maximum=4000 |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-11441d886d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-38ceaab584"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSession`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8273b4a0c12b6fa55f8669267e1ed4d9a0091e43944c85d6f7e1c16aa6e892ac -->

```json
{
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
```

</details>
