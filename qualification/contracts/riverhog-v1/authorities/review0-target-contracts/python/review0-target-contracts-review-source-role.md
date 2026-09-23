# review0_target_contracts.REVIEW_SOURCE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-source-role:b5e95b2aca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2fe8eebe7"></a>
- <a id="s-ab89e0c326"></a>`distribution`: `review0-target-contracts`
- <a id="s-3a2491bdf8"></a>`module`: `review0_target_contracts`
- <a id="s-e07a79cdf2"></a>`name`: `REVIEW_SOURCE_ROLE`
- <a id="s-c2daa08b6c"></a>`unit`: `export`

### Declared structure

- <a id="s-7d56b3d0cf"></a>`kind`: `"constant"`
- <a id="s-de066e2991"></a>`value`: `"stove0.review.source/v1"`

## Governing policies

- <a id="pa-adcf019c98"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_SOURCE_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f91593c57f8f18d27691fc682c871a5686f5334851bc865b2d720a61fcfb216 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.source/v1"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_SOURCE_ROLE",
  "unit": "export"
}
```

</details>
