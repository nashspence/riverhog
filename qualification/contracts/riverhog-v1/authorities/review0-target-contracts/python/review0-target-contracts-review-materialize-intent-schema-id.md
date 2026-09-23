# review0_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-materiali-df6f92bbad:a82671ab10 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-398eebe583"></a>
- <a id="s-ff78287547"></a>`distribution`: `review0-target-contracts`
- <a id="s-fc229fd860"></a>`module`: `review0_target_contracts`
- <a id="s-ea59a23d2d"></a>`name`: `REVIEW_MATERIALIZE_INTENT_SCHEMA_ID`
- <a id="s-66b3a5c565"></a>`unit`: `export`

### Declared structure

- <a id="s-0d5f3bd933"></a>`kind`: `"constant"`
- <a id="s-78657a45bb"></a>`value`: `"stove0.review.materialize-intent/v1"`

## Governing policies

- <a id="pa-8319a9a277"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 749bc8b76df11667c6a0c19c83c68540ef49e86bb6fe5f202a16f6700faae74d -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.materialize-intent/v1"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_SCHEMA_ID",
  "unit": "export"
}
```

</details>
