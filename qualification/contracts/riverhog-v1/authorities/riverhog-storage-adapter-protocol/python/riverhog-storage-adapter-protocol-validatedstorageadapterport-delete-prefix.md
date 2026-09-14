# riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.delete_prefix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-d80b9332cf:ec37a0a8e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a659e43bec"></a>
- <a id="s-da94c2fdfb"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-4e703a6347"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-2ddba457ae"></a>`name`: `delete_prefix`
- <a id="s-8df2a1dc53"></a>`owner`: `riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort`
- <a id="s-268eba1daa"></a>`unit`: `member`

### Declared structure

- <a id="s-dfd1d50112"></a>`kind`: `"method"`
- <a id="s-ab19b828d0"></a>`signature`: `"\"(self, request: 'DeletePrefixRequest') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort](riverhog-storage-adapter-protocol-validatedstorageadapterport.md)

## Governing policies

- <a id="pa-dcad6b666f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.delete_prefix`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f58f819b517ee02d38668d3fa65279ac06ef9b05beca6cfa846e063396616f1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'DeletePrefixRequest') -> 'int'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "delete_prefix",
  "owner": "riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort",
  "unit": "member"
}
```
