# riverhog_storage_adapter_support.StorageAdapterClient.begin_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-b2679d37f2:c98fadc4f0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-efc2477b2e"></a>
- <a id="s-de566600a0"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-3c0ddc2f9f"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-d8dd5db516"></a>`name`: `begin_write`
- <a id="s-6479fda695"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-bc764ba996"></a>`unit`: `member`

### Declared structure

- <a id="s-ba4cd2175b"></a>`kind`: `"method"`
- <a id="s-b619688e5f"></a>`signature`: `"\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-a5deebfde1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.begin_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d972fd91c6fca3490fada6e9883612662290b56016736c1c50e64ebe71ee9611 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "begin_write",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
