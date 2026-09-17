# gogurt_core.validate_gogurt_interval

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-validate-gogurt-interval:af4e6dbb76 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-60b9cbb23d"></a>
- <a id="s-ea7072826e"></a>`distribution`: `gogurt-core`
- <a id="s-88c1faea5b"></a>`module`: `gogurt_core`
- <a id="s-f65fb53b3b"></a>`name`: `validate_gogurt_interval`
- <a id="s-08c66076f8"></a>`unit`: `export`

### Declared structure

- <a id="s-4c8ef19253"></a>`kind`: `"function"`
- <a id="s-ddc1d3e36a"></a>`signature`: `"\"(value: 'object') -> 'float'\""`

## Governing policies

- <a id="pa-1944e3c57e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.validate_gogurt_interval`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e7ccb52179d5877d876fd7e895874bdccf9afbc4152b22d6e94b7c47b7fdcab -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'float'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "validate_gogurt_interval",
  "unit": "export"
}
```

</details>
