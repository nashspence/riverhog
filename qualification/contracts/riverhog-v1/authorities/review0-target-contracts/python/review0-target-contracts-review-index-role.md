# review0_target_contracts.REVIEW_INDEX_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-index-role:48aed65a87 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-14b77f4e2c"></a>
- <a id="s-4801823c38"></a>`distribution`: `review0-target-contracts`
- <a id="s-d4b08c5819"></a>`module`: `review0_target_contracts`
- <a id="s-848ee9469b"></a>`name`: `REVIEW_INDEX_ROLE`
- <a id="s-cce95e9090"></a>`unit`: `export`

### Declared structure

- <a id="s-ba09325fe0"></a>`kind`: `"constant"`
- <a id="s-3f4071dfef"></a>`value`: `"stove0.review.index/v1"`

## Governing policies

- <a id="pa-01c6215111"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_INDEX_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8013b71ea7fb7d6fd4bee4c526c937b6ab7850cbd024e812e9801d1555a801df -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.index/v1"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_INDEX_ROLE",
  "unit": "export"
}
```

</details>
