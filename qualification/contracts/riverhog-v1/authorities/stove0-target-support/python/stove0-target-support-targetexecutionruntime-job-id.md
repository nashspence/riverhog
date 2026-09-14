# stove0_target_support.TargetExecutionRuntime.job_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionruntime-job-id:62dde8d59f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-befc09123f"></a>
- <a id="s-e3f8ef96cd"></a>`distribution`: `stove0-target-support`
- <a id="s-ffcb4e23e8"></a>`module`: `stove0_target_support`
- <a id="s-3a90077a48"></a>`name`: `job_id`
- <a id="s-ada5dd7cc7"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-e03f545128"></a>`unit`: `member`

### Declared structure

- <a id="s-1f62700d81"></a>`kind`: `"property"`
- <a id="s-f9d8ccca9c"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-39c6e4e6e3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.job_id`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34f1673a49050fc206855eafeebcf2a5aafae5e20630e7aef2e9b46143c7072a -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "job_id",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```
