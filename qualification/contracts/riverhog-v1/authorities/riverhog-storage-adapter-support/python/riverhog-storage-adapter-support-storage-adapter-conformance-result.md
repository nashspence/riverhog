# riverhog_storage_adapter_support.STORAGE_ADAPTER_CONFORMANCE_RESULT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storage-30c1dcd9d4:a8fe3a9245 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c3ba82b74"></a>
- <a id="s-f4f18dfff5"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-eed9eeea5e"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-27f4620606"></a>`name`: `STORAGE_ADAPTER_CONFORMANCE_RESULT`
- <a id="s-87afb39c62"></a>`unit`: `export`

### Declared structure

- <a id="s-3de4e8c345"></a>`kind`: `"constant"`
- <a id="s-2cb92f890f"></a>`value`: `"riverhog-storage-adapter-conformance-result/v1"`

## Governing policies

- <a id="pa-0181676391"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.STORAGE_ADAPTER_CONFORMANCE_RESULT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1220a30cf79c090b49ee6d3c5e4525ce039d7a39c170d07c510dff59a7bcf439 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-storage-adapter-conformance-result/v1"
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "STORAGE_ADAPTER_CONFORMANCE_RESULT",
  "unit": "export"
}
```

</details>
