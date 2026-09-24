# a_stove0_media_archive_contract_lib.operation_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-opera-61a01926d9:99e35f414e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4d0f64e49"></a>
- <a id="s-04f091c2e3"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-7b0dc4329a"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-bf3aa09128"></a>`name`: `operation_contract`
- <a id="s-6c544e70c6"></a>`unit`: `export`

### Declared structure

- <a id="s-6b04181dae"></a>`kind`: `"function"`
- <a id="s-8d00b57c77"></a>`signature`: `"\"(operation_id: 'str') -> 'OperationContract'\""`

## Governing policies

- <a id="pa-14ffb78420"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.operation_contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1aaddb25d203b0f0f7e9d8bd15473c89f0b13da7007973baea14508088799fa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(operation_id: 'str') -> 'OperationContract'\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "operation_contract",
  "unit": "export"
}
```

</details>
