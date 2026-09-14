# stove0_target_support.TargetContractPayload.canonical_operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontractpaylo-bc7a574b97:f011e0776a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db3c4f86fb"></a>
- <a id="s-9ddae1792b"></a>`distribution`: `stove0-target-support`
- <a id="s-54f1c7bd10"></a>`module`: `stove0_target_support`
- <a id="s-0bfffa8e3b"></a>`name`: `canonical_operations`
- <a id="s-f1d7e6d7e4"></a>`owner`: `stove0_target_support.TargetContractPayload`
- <a id="s-e64ff63127"></a>`unit`: `member`

### Declared structure

- <a id="s-95f405ac3e"></a>`kind`: `"classmethod"`
- <a id="s-53120cadb9"></a>`signature`: `"\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetContractPayload](stove0-target-support-targetcontractpayload.md)

## Governing policies

- <a id="pa-3d376b6911"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContractPayload.canonical_operations`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a1a039354dae1c472e6a643df84bb66e68b11cbaa4d6bd794d2a609cc5cc643 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_operations",
  "owner": "stove0_target_support.TargetContractPayload",
  "unit": "member"
}
```
