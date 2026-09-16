# riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.find_completed_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-5a773fd098:81d1cc6342 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e7e0ef178"></a>
- <a id="s-36647d0b7c"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-11f31e0e10"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-e4c1ab0edd"></a>`name`: `find_completed_write`
- <a id="s-22d36b6e8f"></a>`owner`: `riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort`
- <a id="s-cd22014dd7"></a>`unit`: `member`

### Declared structure

- <a id="s-87857b566e"></a>`kind`: `"method"`
- <a id="s-47dd822ea0"></a>`signature`: `"\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt \| None'\""`

## Maintained corroboration

### Related interface records

- [ValidatedStorageAdapterPort](riverhog-storage-adapter-protocol-validatedstorageadapterport.md)

## Governing policies

- <a id="pa-2d3bd6025b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.find_completed_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1b10e2e2c6367ef14161cf5a71674ea93bf0d785c576939adc28a96ba0293a4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt | None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "find_completed_write",
  "owner": "riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort",
  "unit": "member"
}
```

</details>
