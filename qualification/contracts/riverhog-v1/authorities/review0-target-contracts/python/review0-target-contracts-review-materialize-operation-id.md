# review0_target_contracts.REVIEW_MATERIALIZE_OPERATION_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-materiali-bb5bf9938a:7946cc7653 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39273eb59e"></a>
- <a id="s-07f841834b"></a>`distribution`: `review0-target-contracts`
- <a id="s-32577e9c1f"></a>`module`: `review0_target_contracts`
- <a id="s-f448f5e576"></a>`name`: `REVIEW_MATERIALIZE_OPERATION_ID`
- <a id="s-e986e3c405"></a>`unit`: `export`

### Declared structure

- <a id="s-c80607884e"></a>`kind`: `"constant"`
- <a id="s-ea7676f40d"></a>`value`: `"stove0.review.materialize/v1"`

## Governing policies

- <a id="pa-201f2086ae"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_MATERIALIZE_OPERATION_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e02e8e77eed22340685cc3418123987b37453706e4085a0ddd9a71e811a48f3 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.materialize/v1"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_MATERIALIZE_OPERATION_ID",
  "unit": "export"
}
```

</details>
