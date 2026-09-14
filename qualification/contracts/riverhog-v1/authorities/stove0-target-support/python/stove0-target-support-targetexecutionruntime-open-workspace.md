# stove0_target_support.TargetExecutionRuntime.open_workspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-9e61bc3797:83178d16c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-abedd7dfda"></a>
- <a id="s-300c376912"></a>`distribution`: `stove0-target-support`
- <a id="s-93ff9c63b6"></a>`module`: `stove0_target_support`
- <a id="s-2f73e138f0"></a>`name`: `open_workspace`
- <a id="s-64348cf858"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-4180fab5ff"></a>`unit`: `member`

### Declared structure

- <a id="s-8d2ac63b02"></a>`kind`: `"method"`
- <a id="s-9435df7d85"></a>`signature`: `"\"(self, root: 'Path') -> 'TransformWorkspace'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-deddacf1f5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.open_workspace`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2140660a58b8d275fe912699a2bb9bad1a08988f4259d8ee71c22aa8ce772893 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, root: 'Path') -> 'TransformWorkspace'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "open_workspace",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```
