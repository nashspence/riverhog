# stove0_target_support.TargetExecutionRuntime.prepare_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-b91d3fad3d:b96180bb8f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e0bbe0d3f0"></a>
- <a id="s-1a252bd00d"></a>`distribution`: `stove0-target-support`
- <a id="s-e65963782b"></a>`module`: `stove0_target_support`
- <a id="s-b8471c746d"></a>`name`: `prepare_inputs`
- <a id="s-6ac0dc1a1f"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-7db56b5fa9"></a>`unit`: `member`

### Declared structure

- <a id="s-d4d9d8f7f1"></a>`kind`: `"method"`
- <a id="s-197d82578b"></a>`signature`: `"\"(self, inputs: 'Sequence[InputArtifact] \| None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""`

## Maintained corroboration

### Related interface records

- [TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-5c9340e515"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.prepare_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a441c728d7c7f61d778866f5cd1dc1401f2cc46d5445512567abe9ec03a1e3aa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, inputs: 'Sequence[InputArtifact] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "prepare_inputs",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```

</details>
