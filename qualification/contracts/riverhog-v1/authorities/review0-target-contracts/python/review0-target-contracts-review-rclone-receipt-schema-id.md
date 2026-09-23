# review0_target_contracts.REVIEW_RCLONE_RECEIPT_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-rclone-re-7d784cdc2f:646b0c9007 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d3e0a98b64"></a>
- <a id="s-c77954fabd"></a>`distribution`: `review0-target-contracts`
- <a id="s-e2153266e4"></a>`module`: `review0_target_contracts`
- <a id="s-d70834282d"></a>`name`: `REVIEW_RCLONE_RECEIPT_SCHEMA_ID`
- <a id="s-5f90e8ce53"></a>`unit`: `export`

### Declared structure

- <a id="s-0044b10105"></a>`kind`: `"constant"`
- <a id="s-83949f1250"></a>`value`: `"stove0.review.rclone-receipt/v1"`

## Governing policies

- <a id="pa-cc78bd1a99"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_RCLONE_RECEIPT_SCHEMA_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51e01e501170e9072e771eaf2a7a4b6ee50eea9206bb813e31df38fec477630e -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.rclone-receipt/v1"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_RCLONE_RECEIPT_SCHEMA_ID",
  "unit": "export"
}
```

</details>
