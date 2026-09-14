# stove0_target_protocol.TransformPlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-transformplan-seal:665db63cbd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-47e27acd26"></a>
- <a id="s-8c1166561c"></a>`distribution`: `stove0-target-protocol`
- <a id="s-3845ca8125"></a>`module`: `stove0_target_protocol`
- <a id="s-52ffc3f044"></a>`name`: `seal`
- <a id="s-4f5f09ef43"></a>`owner`: `stove0_target_protocol.TransformPlan`
- <a id="s-e78d341cee"></a>`unit`: `member`

### Declared structure

- <a id="s-9e9cbbbf61"></a>`kind`: `"classmethod"`
- <a id="s-2fea2ceb16"></a>`signature`: `"\"(cls, payload: 'TransformPlanPayload') -> 'TransformPlan'\""`

## Maintained corroboration

### Related interface records

- [TransformPlan](stove0-target-protocol-transformplan.md)

## Governing policies

- <a id="pa-08b2ba5459"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TransformPlan.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be2295dfaf1eaa00d7e5747f917782066e5d76faba91a5d593993f6b4ab638a3 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'TransformPlanPayload') -> 'TransformPlan'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.TransformPlan",
  "unit": "member"
}
```
