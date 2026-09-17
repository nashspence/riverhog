# stove0_target_support.TargetExecutionFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionfailure:e97bcf4b8f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ab657fbd5"></a>
- <a id="s-11b99ab7e8"></a>`distribution`: `stove0-target-support`
- <a id="s-1a63801789"></a>`module`: `stove0_target_support`
- <a id="s-ff897f6e1e"></a>`name`: `TargetExecutionFailure`
- <a id="s-38d9e20e6b"></a>`unit`: `export`

### Declared structure

- <a id="s-f0e4291ce9"></a>`kind`: `"class"`
- <a id="s-6df9c8580f"></a>`signature`: `"\"(code: 'str', message: 'str', *, retryable: 'bool') -> 'None'\""`

## Governing policies

- <a id="pa-779c1a0e95"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionFailure`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea5cb13d0f370b17a03ef50a163ba98963c53ab8e71abac0aa3448de4a8e8c51 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(code: 'str', message: 'str', *, retryable: 'bool') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetExecutionFailure",
  "unit": "export"
}
```

</details>
