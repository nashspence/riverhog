# review0_target_contracts.ReviewSamplePlan.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsampleplan-208af88081:9c2da747d8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5e1dc2e8d"></a>
- <a id="s-ff947c13ee"></a>`distribution`: `review0-target-contracts`
- <a id="s-e1a99047f2"></a>`module`: `review0_target_contracts`
- <a id="s-5a8a4d7885"></a>`name`: `verify_digest`
- <a id="s-163086e9ef"></a>`owner`: `review0_target_contracts.ReviewSamplePlan`
- <a id="s-f6374edf1d"></a>`unit`: `member`

### Declared structure

- <a id="s-398e43d924"></a>`kind`: `"method"`
- <a id="s-7335022c30"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplePlan](review0-target-contracts-reviewsampleplan.md)

## Governing policies

- <a id="pa-8f045c8080"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSamplePlan.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3566f464d2c30dc3d38162499507fa6756e9a2197563b5849128647fda6fb1a7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "verify_digest",
  "owner": "review0_target_contracts.ReviewSamplePlan",
  "unit": "member"
}
```

</details>
