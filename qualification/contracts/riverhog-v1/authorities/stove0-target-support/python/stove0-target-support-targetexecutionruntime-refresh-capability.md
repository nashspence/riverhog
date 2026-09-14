# stove0_target_support.TargetExecutionRuntime.refresh_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-81bd4e5122:81ca1a6226 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ce15de2f6"></a>
- <a id="s-b1c0b62e1d"></a>`distribution`: `stove0-target-support`
- <a id="s-81a30ad74b"></a>`module`: `stove0_target_support`
- <a id="s-e14bf56ade"></a>`name`: `refresh_capability`
- <a id="s-09647ce340"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-01ef28bae8"></a>`unit`: `member`

### Declared structure

- <a id="s-4d35aa8556"></a>`kind`: `"method"`
- <a id="s-be8e7765df"></a>`signature`: `"\"(self, token: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-746d17ab0d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.refresh_capability`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4eab4f7136ff448a74e26bb68d966e8c6dbb3be4befd173f8cf89965b771a935 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, token: 'str') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "refresh_capability",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```
