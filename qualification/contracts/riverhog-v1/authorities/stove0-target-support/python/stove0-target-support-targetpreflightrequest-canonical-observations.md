# stove0_target_support.TargetPreflightRequest.canonical_observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetpreflightrequ-81afdf1f06:813f358f18 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de4af9f3f3"></a>
| Field | Shape |
|---|---|
| <a id="s-a4119629d2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8b8083726c"></a>`distribution` | "stove0-target-support" |
| <a id="s-ea4f4a4a1a"></a>`module` | "stove0_target_support" |
| <a id="s-da35ad22b9"></a>`name` | "canonical_observations" |
| <a id="s-fad0e5ac60"></a>`owner` | "stove0_target_support.TargetPreflightRequest" |
| <a id="s-32b5574853"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetPreflightRequest](stove0-target-support-targetpreflightrequest.md)

## Governing policies

- <a id="pa-7063b07eee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetPreflightRequest.canonical_observations`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68e08be23765f8c3926d038b9e3a6b3a7a937bdf73357ab0ec11838c577c3c74 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_observations",
  "owner": "stove0_target_support.TargetPreflightRequest",
  "unit": "member"
}
```
