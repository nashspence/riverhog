# riverhog_storage_adapter_protocol.StorageAdapterPort.read_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-3c418951f0:26be8c5ef8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db7a158320"></a>
- <a id="s-c93079b758"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-826502f053"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-a30bea07f7"></a>`name`: `read_object`
- <a id="s-3d03b416e3"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-95fa943dc8"></a>`unit`: `member`

### Declared structure

- <a id="s-3014051020"></a>`kind`: `"method"`
- <a id="s-844a5dbbe7"></a>`signature`: `"\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-c754097323"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.read_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 353e9af6a14a20206fac531b6b73aff8bfb4479bcd34ee97e10ede74c10f43af -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "read_object",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```

</details>
