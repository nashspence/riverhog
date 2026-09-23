# review0_target_contracts.REVIEW_RCLONE_DELIVER_OPERATION_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-rclone-de-ee5c4ec9d5:e9450de05e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f913006307"></a>
- <a id="s-e331a6afeb"></a>`distribution`: `review0-target-contracts`
- <a id="s-e4cbda0f5d"></a>`module`: `review0_target_contracts`
- <a id="s-77274ac916"></a>`name`: `REVIEW_RCLONE_DELIVER_OPERATION_ID`
- <a id="s-4c3278e0fe"></a>`unit`: `export`

### Declared structure

- <a id="s-7d84702df1"></a>`kind`: `"constant"`
- <a id="s-3fccb62853"></a>`value`: `"stove0.review.rclone-deliver/v1"`

## Governing policies

- <a id="pa-1cd96fec81"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_RCLONE_DELIVER_OPERATION_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61b7b3f4c142471cc67ba6e707841082ce6e49697d4f46c7dea661d84e3e0b60 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.rclone-deliver/v1"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_RCLONE_DELIVER_OPERATION_ID",
  "unit": "export"
}
```

</details>
