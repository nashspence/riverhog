# stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-mat-055d8dd392:3f326cdd02 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-82491f4ec6"></a>
- <a id="s-3994e79371"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-dbeedea2aa"></a>`module`: `stove0_review_target_contracts`
- <a id="s-41223c54ba"></a>`name`: `REVIEW_MATERIALIZE_INTENT_SCHEMA_ID`
- <a id="s-614a78e0cf"></a>`unit`: `export`

### Declared structure

- <a id="s-b79e626c3b"></a>`kind`: `"constant"`
- <a id="s-7a7c376196"></a>`value`: `"stove0.review.materialize-intent/v1"`

## Governing policies

- <a id="pa-4b55528ab2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources/authorities.md#src-1d0886e380) — [reference/stove0/targets/review/contracts/src/stove0\_review\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22749fe28790d79683040283f5d194287b10fe7bcb5f08bdd73456d1a782301f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.materialize-intent/v1"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_SCHEMA_ID",
  "unit": "export"
}
```

</details>
