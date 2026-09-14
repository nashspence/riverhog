# stove0_target_protocol.EffectPlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-effectplan-seal:b341a721ce -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8a1cf2f49a"></a>
- <a id="s-6837cc2bc9"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1b67b36bb1"></a>`module`: `stove0_target_protocol`
- <a id="s-5a87b0c58e"></a>`name`: `seal`
- <a id="s-3bc21128ad"></a>`owner`: `stove0_target_protocol.EffectPlan`
- <a id="s-bb284c3df3"></a>`unit`: `member`

### Declared structure

- <a id="s-1fa505f0b1"></a>`kind`: `"classmethod"`
- <a id="s-ab2a2b46f5"></a>`signature`: `"\"(cls, payload: 'EffectPlanPayload') -> 'EffectPlan'\""`

## Maintained corroboration

### Related interface records

- [EffectPlan](stove0-target-protocol-effectplan.md)

## Governing policies

- <a id="pa-1de1c769de"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.EffectPlan.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37dc91b8682f52089fdb8a25193e9c5eab06192e2d90bd201975e281762310c5 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'EffectPlanPayload') -> 'EffectPlan'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.EffectPlan",
  "unit": "member"
}
```
