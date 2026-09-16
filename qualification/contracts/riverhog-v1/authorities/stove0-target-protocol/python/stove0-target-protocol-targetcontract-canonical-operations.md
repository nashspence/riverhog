# stove0_target_protocol.TargetContract.canonical_operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcontract-can-f0c026df6b:5c0decbf76 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e606b36096"></a>
- <a id="s-2128b99439"></a>`distribution`: `stove0-target-protocol`
- <a id="s-3928665242"></a>`module`: `stove0_target_protocol`
- <a id="s-8dbdcdf1ee"></a>`name`: `canonical_operations`
- <a id="s-363bfbc3d6"></a>`owner`: `stove0_target_protocol.TargetContract`
- <a id="s-61b6b83f54"></a>`unit`: `member`

### Declared structure

- <a id="s-defe97975f"></a>`kind`: `"classmethod"`
- <a id="s-d1c1626d78"></a>`signature`: `"\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetContract](stove0-target-protocol-targetcontract.md)

## Governing policies

- <a id="pa-e29eefc6cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetContract.canonical_operations`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d03b6d7757ba00c372d17ea965bc2a79b960023bc1070a8e677f2c9b47f0b8bb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_operations",
  "owner": "stove0_target_protocol.TargetContract",
  "unit": "member"
}
```
