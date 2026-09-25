# riverhog_storage_adapter_support.StorageAdapterClient.pin_incarnation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-46c9ba29ea:742998b4b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5e60579a79"></a>
- <a id="s-858addc137"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-4227aef6aa"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-e1572ebb3d"></a>`name`: `pin_incarnation`
- <a id="s-d6e9cef1f5"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-463efb6710"></a>`unit`: `member`

### Declared structure

- <a id="s-4675bd51a7"></a>`kind`: `"method"`
- <a id="s-bf28f4c8bd"></a>`signature`: `"\"(self, incarnation_id: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-49be9a24cf"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.pin_incarnation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74244cf0ed1f652c3484076e123283cb91a9d7d1b4a4e7d00b39e6a9f6de500a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, incarnation_id: 'str') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "pin_incarnation",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
