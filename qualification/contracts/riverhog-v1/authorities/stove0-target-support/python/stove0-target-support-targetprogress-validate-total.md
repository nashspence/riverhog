# stove0_target_support.TargetProgress.validate_total

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetprogress-validate-total:f4d255b8cc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06ecdebb76"></a>
| Field | Shape |
|---|---|
| <a id="s-adeb2b32be"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-114f52b116"></a>`distribution` | "stove0-target-support" |
| <a id="s-4f4262833f"></a>`module` | "stove0_target_support" |
| <a id="s-c7a13cf4b6"></a>`name` | "validate_total" |
| <a id="s-112d60817d"></a>`owner` | "stove0_target_support.TargetProgress" |
| <a id="s-d9eea9ef4f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetProgress](stove0-target-support-targetprogress.md)

## Governing policies

- <a id="pa-4c09dcfe25"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetProgress.validate_total`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 005a0cca5f76b3d86fd211ae98a03c6833adad3151f10c30d9e169296e4a2dc1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "validate_total",
  "owner": "stove0_target_support.TargetProgress",
  "unit": "member"
}
```
