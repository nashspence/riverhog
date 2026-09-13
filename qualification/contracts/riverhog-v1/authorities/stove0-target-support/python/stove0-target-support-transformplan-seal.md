# stove0_target_support.TransformPlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-transformplan-seal:b649c39d34 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4c90e4d0f4"></a>
| Field | Shape |
|---|---|
| <a id="s-e944503faa"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a3a2b68810"></a>`distribution` | "stove0-target-support" |
| <a id="s-dfe9833bfc"></a>`module` | "stove0_target_support" |
| <a id="s-9847b4b0c7"></a>`name` | "seal" |
| <a id="s-3e929a36d4"></a>`owner` | "stove0_target_support.TransformPlan" |
| <a id="s-ab74454283"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TransformPlan](stove0-target-support-transformplan.md)

## Governing policies

- <a id="pa-8738343f90"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TransformPlan.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3fbbec35a3437aae174931c4c98721d301b23316e53778b08991e9a55802028f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'TransformPlanPayload') -> 'TransformPlan'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "seal",
  "owner": "stove0_target_support.TransformPlan",
  "unit": "member"
}
```
