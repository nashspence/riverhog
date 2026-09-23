# stove0_target_support.TargetExecutionRuntime.iter_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-2730b6f48a:9bf10342f7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b92a33abf3"></a>
- <a id="s-eca64faa57"></a>`distribution`: `stove0-target-support`
- <a id="s-e0dd7a8ee5"></a>`module`: `stove0_target_support`
- <a id="s-8ab01c6ae7"></a>`name`: `iter_inputs`
- <a id="s-7bad1a3979"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-a2ada5fa4a"></a>`unit`: `member`

### Declared structure

- <a id="s-c5c746ef1e"></a>`kind`: `"method"`
- <a id="s-a58da3d58b"></a>`signature`: `"\"(self) -> 'Iterator[tuple[InputArtifact, ClaimedArtifact]]'\""`

## Maintained corroboration

### Related interface records

- [TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-850d6c8eb2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.iter_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a75e0ff0f3fa527a1334ec68b5bf1f178fae1a17e00d3d11442d091b7a036ee -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Iterator[tuple[InputArtifact, ClaimedArtifact]]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "iter_inputs",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```

</details>
