# stove0_protocol.CoordinationSettlement.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationsettlement-seal:59d7c37a3f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a32a471ea2"></a>
- <a id="s-f73200e857"></a>`distribution`: `stove0-protocol`
- <a id="s-9efa6f10f2"></a>`module`: `stove0_protocol`
- <a id="s-907095cabd"></a>`name`: `seal`
- <a id="s-e1ae10c2aa"></a>`owner`: `stove0_protocol.CoordinationSettlement`
- <a id="s-e021fe79e5"></a>`unit`: `member`

### Declared structure

- <a id="s-cd7f725841"></a>`kind`: `"classmethod"`
- <a id="s-308d2e3f8f"></a>`signature`: `"\"(cls, *, plan: 'BranchSetPlan', collection_settlements: 'Sequence[BranchSettlement]', effect_settlements: 'Sequence[BranchEffectSettlement]', coordination_settlements: 'Sequence[CoordinationSettlement]', join_settlement: 'JoinSettlement \| None') -> 'CoordinationSettlement'\""`

## Maintained corroboration

### Related interface records

- [CoordinationSettlement](stove0-protocol-coordinationsettlement.md)

## Governing policies

- <a id="pa-811ba036ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationSettlement.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 300c4249ac44932b97bbc5ed6b737927781de86be0c5ac3772abdee5e55fd485 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, plan: 'BranchSetPlan', collection_settlements: 'Sequence[BranchSettlement]', effect_settlements: 'Sequence[BranchEffectSettlement]', coordination_settlements: 'Sequence[CoordinationSettlement]', join_settlement: 'JoinSettlement | None') -> 'CoordinationSettlement'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.CoordinationSettlement",
  "unit": "member"
}
```

</details>
