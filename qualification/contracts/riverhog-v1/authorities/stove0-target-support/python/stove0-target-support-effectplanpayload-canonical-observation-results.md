# stove0_target_support.EffectPlanPayload.canonical_observation_results

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-effectplanpayload-c-4434cc9441:f80041c4d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2f474b47e5"></a>
| Field | Shape |
|---|---|
| <a id="s-69e7e56021"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-af452ca06a"></a>`distribution` | "stove0-target-support" |
| <a id="s-5e5b11a332"></a>`module` | "stove0_target_support" |
| <a id="s-ecf2402764"></a>`name` | "canonical_observation_results" |
| <a id="s-ae927f1ffe"></a>`owner` | "stove0_target_support.EffectPlanPayload" |
| <a id="s-4c21c257db"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.EffectPlanPayload](stove0-target-support-effectplanpayload.md)

## Governing policies

- <a id="pa-4378bfe6b0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.EffectPlanPayload.canonical_observation_results`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dae286aa8b7099a79e974873a58a01ac7ff716c6e8ddb4c645a9c9c998cfe637 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_observation_results",
  "owner": "stove0_target_support.EffectPlanPayload",
  "unit": "member"
}
```
