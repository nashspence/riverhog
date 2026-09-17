# stove0_target_support.TARGET_HTTP_OPERATIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-target-http-operations:757c74fdf7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91e44692dc"></a>
- <a id="s-b6351abbd1"></a>`distribution`: `stove0-target-support`
- <a id="s-a5b3b8e4f4"></a>`module`: `stove0_target_support`
- <a id="s-a8fb3508b8"></a>`name`: `TARGET_HTTP_OPERATIONS`
- <a id="s-50e1e8260c"></a>`unit`: `export`

### Declared structure

- <a id="s-805858c57d"></a>`kind`: `"object"`
- <a id="s-3e665e17a3"></a>`type`: `"builtins.tuple"`

## Governing policies

- <a id="pa-05a3629e63"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TARGET_HTTP_OPERATIONS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 627592b4120c2ceece9143c2475efba194444eb346e7f55d4d64e9a64698124c -->

```json
{
  "contract": {
    "kind": "object",
    "type": "builtins.tuple"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TARGET_HTTP_OPERATIONS",
  "unit": "export"
}
```

</details>
