# riverhog_storage_adapter_s3_support.create_s3_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-create-s3-client:6c44071135 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9da029486"></a>
- <a id="s-106d611bfe"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-fa9289f897"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-3175926e69"></a>`name`: `create_s3_client`
- <a id="s-e5981352dd"></a>`unit`: `export`

### Declared structure

- <a id="s-fe3c687516"></a>`kind`: `"function"`
- <a id="s-b32724c17e"></a>`signature`: `"\"(config: 'S3ClientConfig', *, tuning: 'S3TransportTuning \| None' = None) -> 'Any'\""`

## Governing policies

- <a id="pa-fa725dd093"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources/authorities.md#src-aa14de5031) — [some-implementations/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.create_s3_client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8602d6cc12f9c5074bd02f90610fd0b05d12e743e4f0a1e3c814dcbc35fcdf9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config: 'S3ClientConfig', *, tuning: 'S3TransportTuning | None' = None) -> 'Any'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "create_s3_client",
  "unit": "export"
}
```

</details>
