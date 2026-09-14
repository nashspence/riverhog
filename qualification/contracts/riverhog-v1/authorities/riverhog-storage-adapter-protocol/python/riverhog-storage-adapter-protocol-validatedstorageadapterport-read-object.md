# riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.read_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-87d3a4a95e:ae819590cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39b38c8293"></a>
- <a id="s-e160e9f53e"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-1adfbe056f"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-f7a39be39c"></a>`name`: `read_object`
- <a id="s-5869e2f45c"></a>`owner`: `riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort`
- <a id="s-2ed11f5a89"></a>`unit`: `member`

### Declared structure

- <a id="s-5914812f62"></a>`kind`: `"method"`
- <a id="s-84469dee9b"></a>`signature`: `"\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort](riverhog-storage-adapter-protocol-validatedstorageadapterport.md)

## Governing policies

- <a id="pa-a65dd2f7e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.read_object`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8304e891bf1772ead29465a5a406e1402ce6aeae8fe36f76fd3fe30cdd078391 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "read_object",
  "owner": "riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort",
  "unit": "member"
}
```
