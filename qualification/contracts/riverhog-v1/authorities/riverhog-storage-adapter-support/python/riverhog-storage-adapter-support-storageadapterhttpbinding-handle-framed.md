# riverhog_storage_adapter_support.StorageAdapterHttpBinding.handle_framed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-118e8126c3:f8eaa64714 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1c40639669"></a>
- <a id="s-231dae19bf"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-588f37371b"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-d89648ce7f"></a>`name`: `handle_framed`
- <a id="s-4a6e2331c3"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterHttpBinding`
- <a id="s-226359bd05"></a>`unit`: `member`

### Declared structure

- <a id="s-eb1763545a"></a>`kind`: `"method"`
- <a id="s-5dfde32872"></a>`signature`: `"\"(self, method: 'str', path: 'str', chunks: 'Iterable[bytes]', *, content_length: 'int \| None') -> 'StorageAdapterHttpResponse'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterHttpBinding](riverhog-storage-adapter-support-storageadapterhttpbinding.md)

## Governing policies

- <a id="pa-5a7ec9b6da"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterHttpBinding.handle_framed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f32770519df8b248be5a67411951b81ed69c5864d6ef1891af0d5ee553e402b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, method: 'str', path: 'str', chunks: 'Iterable[bytes]', *, content_length: 'int | None') -> 'StorageAdapterHttpResponse'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "handle_framed",
  "owner": "riverhog_storage_adapter_support.StorageAdapterHttpBinding",
  "unit": "member"
}
```

</details>
