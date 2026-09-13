# stove0_target_support.TargetExecutionRuntime.prepare_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-b91d3fad3d:b96180bb8f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e0bbe0d3f0"></a>
| Field | Shape |
|---|---|
| <a id="s-63707e7209"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1a252bd00d"></a>`distribution` | "stove0-target-support" |
| <a id="s-e65963782b"></a>`module` | "stove0_target_support" |
| <a id="s-b8471c746d"></a>`name` | "prepare_inputs" |
| <a id="s-6ac0dc1a1f"></a>`owner` | "stove0_target_support.TargetExecutionRuntime" |
| <a id="s-7db56b5fa9"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-5c9340e515"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.prepare_inputs`

### Exact owned JSON

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
