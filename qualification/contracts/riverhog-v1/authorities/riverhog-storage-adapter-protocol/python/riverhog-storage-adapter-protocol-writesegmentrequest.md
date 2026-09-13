# riverhog_storage_adapter_protocol.WriteSegmentRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writese-46ea2c26f4:73e4c923d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9919f8e86c"></a>
| Field | Shape |
|---|---|
| <a id="s-2dfae28c86"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-07eef77820"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-685571e12f"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-d15cd24c47"></a>`name` | "WriteSegmentRequest" |
| <a id="s-b7039396c9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-eecdcac42d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 741da152461056171d7a20b3ed3b5a252545e876f9c57c3ba80981ea40198359 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e51e90e06c0f6a6ca83920b56cbb0696ced3950650ea8d743c33191cb2cf7d31",
    "signature": "'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, number: Annotated[int, Ge(ge=1)], stored_bytes: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSegmentRequest",
  "unit": "export"
}
```
