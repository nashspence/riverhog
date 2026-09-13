# riverhog_storage_adapter_protocol.WriteSession

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writesession:091526f7c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4739bfe396"></a>
| Field | Shape |
|---|---|
| <a id="s-5f7021a5c9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-cd3f005fb9"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-aff1663f8b"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-a739a910a0"></a>`name` | "WriteSession" |
| <a id="s-787c3950e7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.WriteSession.canonical_path](riverhog-storage-adapter-protocol-writesession-canonical-path.md)

## Governing policies

- <a id="pa-f6dba90dbe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSession`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82d46fee34cd5390839e92811bee6b706dabce203750d964c07a954d97bcd157 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8273b4a0c12b6fa55f8669267e1ed4d9a0091e43944c85d6f7e1c16aa6e892ac",
    "signature": "'(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: Annotated[int, Ge(ge=1)], write_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSession",
  "unit": "export"
}
```
