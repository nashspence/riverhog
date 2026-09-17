# stove0_review_target_contracts.REVIEW_RCLONE_RECEIPT_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-rcl-7a38e1eab4:d3cfa6d47e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-748b3b86d5"></a>
- <a id="s-6129da63d1"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-4ad19fe321"></a>`module`: `stove0_review_target_contracts`
- <a id="s-7d152e69db"></a>`name`: `REVIEW_RCLONE_RECEIPT_SCHEMA_ID`
- <a id="s-f697cb6f21"></a>`unit`: `export`

### Declared structure

- <a id="s-6aeb9e6b48"></a>`kind`: `"constant"`
- <a id="s-d67e2f7139"></a>`value`: `"stove0.review.rclone-receipt/v1"`

## Governing policies

- <a id="pa-7526d19494"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — [reference/stove0/targets/review/contracts/src/stove0\_review\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_RCLONE_RECEIPT_SCHEMA_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b9f931a0f6835c2c6bea369d5acae7b790fff56d29bd92de51a31aa5e806ae1 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.rclone-receipt/v1"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_RCLONE_RECEIPT_SCHEMA_ID",
  "unit": "export"
}
```

</details>
