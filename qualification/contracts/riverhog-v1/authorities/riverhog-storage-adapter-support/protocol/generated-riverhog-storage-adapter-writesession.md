# generated:riverhog-storage-adapter: WriteSession

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writesession:c956556895 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-5d276c77f516"></a>
- <a id="s-9a6acf1beedf"></a>`title`: WriteSession
- <a id="s-514d7def0ee5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b573b398f337"></a>`expected_bytes` | yes | type="integer"; minimum=1 | Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal. |
| <a id="s-f382e9402683"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-bb12b9b0065d"></a>`write_token` | yes | type="string"; minLength=1; maxLength=4000 | Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal. |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field object_path](#s-f382e9402683) | `length · characters · contract_max` | maximum=4096 |
| [field write_token](#s-bb12b9b0065d) | `length · characters · contract_max` | maximum=4000 |

## Governing policies

- <a id="pa-340c44203621"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-b81bf26f697c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSession`

### Exact owned JSON

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
