# generated:riverhog-storage-adapter: WriteSession

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writesession:c956556895 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: WriteSession
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `expected_bytes` | yes | type="integer"; minimum=1 | Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal. |
| `object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| `write_token` | yes | type="string"; minLength=1; maxLength=4000 | Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal. |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

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
