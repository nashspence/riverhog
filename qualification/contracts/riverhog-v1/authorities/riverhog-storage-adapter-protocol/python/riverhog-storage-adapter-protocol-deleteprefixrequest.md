# riverhog_storage_adapter_protocol.DeletePrefixRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-deletep-cf1b6760cd:e0041a9c72 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b80efa5439"></a>
| Field | Shape |
|---|---|
| <a id="s-1d6ed36c80"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-956f06853e"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-be1745cefe"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-07c093b23c"></a>`name` | "DeletePrefixRequest" |
| <a id="s-de499e6350"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.DeletePrefixRequest.canonical_prefix](riverhog-storage-adapter-protocol-deleteprefixrequest-canonical-prefix.md)

## Governing policies

- <a id="pa-99fd68a241"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.DeletePrefixRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c130ffd62868b9ab607b652fa8296ab62d7defde41e0f5fdecb9994a52df9fc6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "26f1798a7e22e1a72e0c6932bfbe66dca3e80273a417c3e5c18fdc0e4bd59a3f",
    "signature": "\"(*, object_prefix: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], mode: Literal['all_versions'] = 'all_versions') -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "DeletePrefixRequest",
  "unit": "export"
}
```
