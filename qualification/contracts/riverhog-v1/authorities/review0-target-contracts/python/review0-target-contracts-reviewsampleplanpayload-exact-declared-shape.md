# review0_target_contracts.ReviewSamplePlanPayload.exact_declared_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsampleplan-22fed4d1b0:edbf28a4ed -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b23b6ce0e6"></a>
- <a id="s-1ff927a3e7"></a>`distribution`: `review0-target-contracts`
- <a id="s-d9e77e9480"></a>`module`: `review0_target_contracts`
- <a id="s-7bbe1ecb32"></a>`name`: `exact_declared_shape`
- <a id="s-fd239b0f5b"></a>`owner`: `review0_target_contracts.ReviewSamplePlanPayload`
- <a id="s-19ef1d3d94"></a>`unit`: `member`

### Declared structure

- <a id="s-7d36063664"></a>`kind`: `"method"`
- <a id="s-a328e1c069"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplePlanPayload](review0-target-contracts-reviewsampleplanpayload.md)

## Governing policies

- <a id="pa-9944d42a9d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSamplePlanPayload.exact_declared_shape`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d29467fc477338d8e82edf71c24b55dc926f96d30ddb6986ad20e0f7d40abc42 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "exact_declared_shape",
  "owner": "review0_target_contracts.ReviewSamplePlanPayload",
  "unit": "member"
}
```

</details>
