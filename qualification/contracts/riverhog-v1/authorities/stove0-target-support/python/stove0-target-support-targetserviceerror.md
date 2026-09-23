# stove0_target_support.TargetServiceError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetserviceerror:e76a9c6f64 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e68edfd904"></a>
- <a id="s-614b4d63f4"></a>`distribution`: `stove0-target-support`
- <a id="s-28fb93053e"></a>`module`: `stove0_target_support`
- <a id="s-20cee0ea29"></a>`name`: `TargetServiceError`
- <a id="s-10d43362a9"></a>`unit`: `export`

### Declared structure

- <a id="s-73d6128442"></a>`kind`: `"class"`
- <a id="s-2b6b833988"></a>`signature`: `"\"(status: 'int', code: 'TargetHttpErrorCode', message: 'str') -> 'None'\""`

## Governing policies

- <a id="pa-da00a1e188"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetServiceError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdc8c6e8cadca325f7352c20bbfe4964e5e765f1b2cdf45a0cba1b9864c04226 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(status: 'int', code: 'TargetHttpErrorCode', message: 'str') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetServiceError",
  "unit": "export"
}
```

</details>
