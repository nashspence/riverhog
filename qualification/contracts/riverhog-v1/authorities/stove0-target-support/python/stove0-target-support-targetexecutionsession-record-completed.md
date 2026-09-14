# stove0_target_support.TargetExecutionSession.record_completed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionsess-4eecab941f:b5a78b415c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a727891969"></a>
- <a id="s-bcd674516c"></a>`distribution`: `stove0-target-support`
- <a id="s-d8f86387f0"></a>`module`: `stove0_target_support`
- <a id="s-5ab46a415a"></a>`name`: `record_completed`
- <a id="s-8e428acec9"></a>`owner`: `stove0_target_support.TargetExecutionSession`
- <a id="s-754ff61245"></a>`unit`: `member`

### Declared structure

- <a id="s-97580dc794"></a>`kind`: `"method"`
- <a id="s-a922cd509f"></a>`signature`: `"\"(self, status: 'TargetJobStatus') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetExecutionSession](stove0-target-support-targetexecutionsession.md)

## Governing policies

- <a id="pa-86f8e778bd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionSession.record_completed`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fee21e4520d88248a3a03749a40131ecf38822a83743e61bc90b85d769dea557 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, status: 'TargetJobStatus') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "record_completed",
  "owner": "stove0_target_support.TargetExecutionSession",
  "unit": "member"
}
```
