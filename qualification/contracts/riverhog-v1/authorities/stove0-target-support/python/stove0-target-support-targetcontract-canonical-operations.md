# stove0_target_support.TargetContract.canonical_operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontract-cano-0d7822c93b:4f43a22644 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a6daee16a"></a>
- <a id="s-a3c5f08a60"></a>`distribution`: `stove0-target-support`
- <a id="s-90c6644fcb"></a>`module`: `stove0_target_support`
- <a id="s-c4fe490443"></a>`name`: `canonical_operations`
- <a id="s-c129650892"></a>`owner`: `stove0_target_support.TargetContract`
- <a id="s-47bfa5a702"></a>`unit`: `member`

### Declared structure

- <a id="s-d8d5dd3aff"></a>`kind`: `"classmethod"`
- <a id="s-f315752eee"></a>`signature`: `"\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetContract](stove0-target-support-targetcontract.md)

## Governing policies

- <a id="pa-63db50f09e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContract.canonical_operations`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1de2e6796768101b9a1019f66703ff026f6f32c54bf7e9117d75c1c38a8b7e9 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_operations",
  "owner": "stove0_target_support.TargetContract",
  "unit": "member"
}
```
