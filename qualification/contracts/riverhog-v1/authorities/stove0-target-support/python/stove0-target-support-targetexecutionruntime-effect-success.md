# stove0_target_support.TargetExecutionRuntime.effect_success

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-4d65ae5d8e:9caf6af479 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-87fa1c8077"></a>
- <a id="s-bdd2d69b0b"></a>`distribution`: `stove0-target-support`
- <a id="s-b665e08529"></a>`module`: `stove0_target_support`
- <a id="s-c2dcf5b94d"></a>`name`: `effect_success`
- <a id="s-c657050562"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-ea00dc2d9f"></a>`unit`: `member`

### Declared structure

- <a id="s-351245ad68"></a>`kind`: `"method"`
- <a id="s-3c7a57a6e4"></a>`signature`: `"\"(self, result: 'Mapping[str, JsonValue]', *, operation: 'OperationContract', execution_sha256: 'str', attempt: 'int' = 1, runtime_evidence: 'Mapping[str, object] \| None' = None) -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-f0ac939447"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.effect_success`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 24cca06631c8996a101948b37294c0f70e3d7f491808027fe6e7fc06f19f9818 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, result: 'Mapping[str, JsonValue]', *, operation: 'OperationContract', execution_sha256: 'str', attempt: 'int' = 1, runtime_evidence: 'Mapping[str, object] | None' = None) -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "effect_success",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```
