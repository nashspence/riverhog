# stove0_target_support.TargetExecutionSession.completed_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionsess-0d843e0dde:1944ec7b0c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e7689264a"></a>
| Field | Shape |
|---|---|
| <a id="s-d7c314e7c2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d728863390"></a>`distribution` | "stove0-target-support" |
| <a id="s-84a779bac1"></a>`module` | "stove0_target_support" |
| <a id="s-f70580b5fc"></a>`name` | "completed_status" |
| <a id="s-abc8cd1f74"></a>`owner` | "stove0_target_support.TargetExecutionSession" |
| <a id="s-52b4addf38"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetExecutionSession](stove0-target-support-targetexecutionsession.md)

## Governing policies

- <a id="pa-30ce6569ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionSession.completed_status`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c72a8c76c7775be8e152d97352bfab9320f220494888b6037dbfda546753e5d -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'TargetJobStatus | None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "completed_status",
  "owner": "stove0_target_support.TargetExecutionSession",
  "unit": "member"
}
```
