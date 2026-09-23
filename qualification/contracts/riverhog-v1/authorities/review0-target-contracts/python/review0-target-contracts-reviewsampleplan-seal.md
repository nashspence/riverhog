# review0_target_contracts.ReviewSamplePlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsampleplan-seal:fb7f594ccd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ddd07ca08b"></a>
- <a id="s-012803e62d"></a>`distribution`: `review0-target-contracts`
- <a id="s-06e5cf8310"></a>`module`: `review0_target_contracts`
- <a id="s-651de2a149"></a>`name`: `seal`
- <a id="s-05299d3b3e"></a>`owner`: `review0_target_contracts.ReviewSamplePlan`
- <a id="s-c9e496ffaa"></a>`unit`: `member`

### Declared structure

- <a id="s-74a5eb8432"></a>`kind`: `"classmethod"`
- <a id="s-af3c35e33d"></a>`signature`: `"\"(cls, payload: 'ReviewSamplePlanPayload') -> 'ReviewSamplePlan'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplePlan](review0-target-contracts-reviewsampleplan.md)

## Governing policies

- <a id="pa-3efb5d0953"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSamplePlan.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: feb48dc9a13d5bf3f3e71a7e8b2d46489fd8dd3dad2dfcec7e2b119d1c062c22 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ReviewSamplePlanPayload') -> 'ReviewSamplePlan'\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "seal",
  "owner": "review0_target_contracts.ReviewSamplePlan",
  "unit": "member"
}
```

</details>
