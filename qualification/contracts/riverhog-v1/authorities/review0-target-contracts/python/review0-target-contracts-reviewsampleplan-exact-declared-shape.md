# review0_target_contracts.ReviewSamplePlan.exact_declared_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsampleplan-2a6482fd78:0fac5ef8fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24f6706602"></a>
- <a id="s-2e8f9e7714"></a>`distribution`: `review0-target-contracts`
- <a id="s-dbc7d57f39"></a>`module`: `review0_target_contracts`
- <a id="s-60a600d9e8"></a>`name`: `exact_declared_shape`
- <a id="s-096b731b1e"></a>`owner`: `review0_target_contracts.ReviewSamplePlan`
- <a id="s-7eb5d5eb6c"></a>`unit`: `member`

### Declared structure

- <a id="s-e1fb442dc5"></a>`kind`: `"method"`
- <a id="s-e417a21a77"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplePlan](review0-target-contracts-reviewsampleplan.md)

## Governing policies

- <a id="pa-d43fa7d842"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSamplePlan.exact_declared_shape`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 493a1c7b31da3c4c3fb88fff0f6a2458a8da17a2cf45914b33e6cdf2d86e839f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "exact_declared_shape",
  "owner": "review0_target_contracts.ReviewSamplePlan",
  "unit": "member"
}
```

</details>
