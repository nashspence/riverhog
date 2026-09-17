# stove0_target_support.TargetExecutionRuntime.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionruntime-exit:55a5625370 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-abdbfe1517"></a>
- <a id="s-a9b85af8b0"></a>`distribution`: `stove0-target-support`
- <a id="s-40f46d91a4"></a>`module`: `stove0_target_support`
- <a id="s-3d0ac00b98"></a>`name`: `__exit__`
- <a id="s-bba8a209fb"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-79f595c5cd"></a>`unit`: `member`

### Declared structure

- <a id="s-c5cb599d1b"></a>`kind`: `"method"`
- <a id="s-25a345cee7"></a>`signature`: `"\"(self, exc_type: 'object', exc: 'object', tb: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-edb85aa335"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4977b93f31b08de0b2c76d663ffa3c1729c8fa1e411131704dd8618979b1fbe8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, exc_type: 'object', exc: 'object', tb: 'object') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "__exit__",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```

</details>
