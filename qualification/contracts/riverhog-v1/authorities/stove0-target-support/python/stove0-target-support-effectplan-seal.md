# stove0_target_support.EffectPlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-effectplan-seal:415bdf28cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb5a8b6f70"></a>
- <a id="s-3aaf55d1a4"></a>`distribution`: `stove0-target-support`
- <a id="s-36e665e0e5"></a>`module`: `stove0_target_support`
- <a id="s-0b723f0463"></a>`name`: `seal`
- <a id="s-f29600ddf3"></a>`owner`: `stove0_target_support.EffectPlan`
- <a id="s-3f5430b180"></a>`unit`: `member`

### Declared structure

- <a id="s-cf0a31f2f3"></a>`kind`: `"classmethod"`
- <a id="s-fe79a06dea"></a>`signature`: `"\"(cls, payload: 'EffectPlanPayload') -> 'EffectPlan'\""`

## Maintained corroboration

### Related interface records

- [EffectPlan](stove0-target-support-effectplan.md)

## Governing policies

- <a id="pa-8eaaeab702"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.EffectPlan.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51dd569dbf96f3b467f72d46ec6fb955aef7a529e173a3f235304b37bf2be9c7 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'EffectPlanPayload') -> 'EffectPlan'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "seal",
  "owner": "stove0_target_support.EffectPlan",
  "unit": "member"
}
```

</details>
