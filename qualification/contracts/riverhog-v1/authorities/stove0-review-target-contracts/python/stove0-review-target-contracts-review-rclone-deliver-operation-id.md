# stove0_review_target_contracts.REVIEW_RCLONE_DELIVER_OPERATION_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-rcl-c7fc0ad835:3824c325c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a15fb13f6e"></a>
- <a id="s-84d0b32e06"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-531ec8845b"></a>`module`: `stove0_review_target_contracts`
- <a id="s-8574463019"></a>`name`: `REVIEW_RCLONE_DELIVER_OPERATION_ID`
- <a id="s-9071553635"></a>`unit`: `export`

### Declared structure

- <a id="s-526eac3dcf"></a>`kind`: `"constant"`
- <a id="s-ef7252a012"></a>`value`: `"stove0.review.rclone-deliver/v1"`

## Governing policies

- <a id="pa-f0c7fedf8d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources/authorities.md#src-1d0886e380) — [reference/stove0/targets/review/contracts/src/stove0\_review\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_RCLONE_DELIVER_OPERATION_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f3d5501f9e94595e7ae55439474b6b26df181abfc3e0f9f24c37ada7c6d88a5 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.rclone-deliver/v1"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_RCLONE_DELIVER_OPERATION_ID",
  "unit": "export"
}
```

</details>
