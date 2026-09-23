# review0_target_lib.file_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-file-identity:9ecb9b6c32 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92c5e11b41"></a>
- <a id="s-25c589ae03"></a>`distribution`: `review0-target-lib`
- <a id="s-50bdccb0a4"></a>`module`: `review0_target_lib`
- <a id="s-3029afa2f4"></a>`name`: `file_identity`
- <a id="s-efbe9b95fe"></a>`unit`: `export`

### Declared structure

- <a id="s-7e357bb228"></a>`kind`: `"function"`
- <a id="s-bb1e474f9e"></a>`signature`: `"\"(path: 'Path') -> 'tuple[int, str]'\""`

## Governing policies

- <a id="pa-ffd784a88e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.file_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 10dadf02b20d44b39867403c6a0610df9dd14d4d776e7c6b1dcbcb0b54e3a5d9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path') -> 'tuple[int, str]'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "file_identity",
  "unit": "export"
}
```

</details>
