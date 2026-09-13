# riverhog_storage_adapter_protocol.ReadPreparationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readpre-8dcef0ea20:79f93417b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b46635c802"></a>
| Field | Shape |
|---|---|
| <a id="s-74c7f03bc6"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4c45dd9185"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-403e0455b1"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-cc9088dc29"></a>`name` | "ReadPreparationRequest" |
| <a id="s-f6561e4d4e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ReadPreparationRequest.canonical_objects](riverhog-storage-adapter-protocol-readpreparationrequest-canonical-objects.md)

## Governing policies

- <a id="pa-56884d6212"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadPreparationRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4aef917af36c1e1440cd78e0ff24035d24ef732d45373f457154c5f3cdc804d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a1096dde3495131448c778944da805d24cbfabda7630aa9d927684235cb42635",
    "signature": "'(*, objects: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.ObjectLocator, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadPreparationRequest",
  "unit": "export"
}
```
