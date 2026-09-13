# stove0_target_support.TargetContract.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontract-verify-digest:56a1962226 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d91b6cb23"></a>
| Field | Shape |
|---|---|
| <a id="s-138a328d11"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f1f5288a7d"></a>`distribution` | "stove0-target-support" |
| <a id="s-a8cd856ae7"></a>`module` | "stove0_target_support" |
| <a id="s-8dd3d5031e"></a>`name` | "verify_digest" |
| <a id="s-859ecbfdf8"></a>`owner` | "stove0_target_support.TargetContract" |
| <a id="s-a1d5926898"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetContract](stove0-target-support-targetcontract.md)

## Governing policies

- <a id="pa-d5805262be"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContract.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ac5f57499dbdddcff1851b5b7664ec9a670ea04ae20272d9d3dde1219775daa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "verify_digest",
  "owner": "stove0_target_support.TargetContract",
  "unit": "member"
}
```
