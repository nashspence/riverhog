# review0_target_contracts.REVIEW_VIDEO_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-video-role:49961bd022 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75393a0de6"></a>
- <a id="s-aa8e15a97a"></a>`distribution`: `review0-target-contracts`
- <a id="s-a104f9b9b3"></a>`module`: `review0_target_contracts`
- <a id="s-b6a82d9d50"></a>`name`: `REVIEW_VIDEO_ROLE`
- <a id="s-7394dc7fef"></a>`unit`: `export`

### Declared structure

- <a id="s-17bbb5f4f0"></a>`kind`: `"constant"`
- <a id="s-85a3284602"></a>`value`: `"stove0.review.video/v1"`

## Governing policies

- <a id="pa-08a6bd4e1f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_VIDEO_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d146af551f6b7ba27c3402d3a18337f0f26e0b856985188eec150c04fa8cf261 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.video/v1"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_VIDEO_ROLE",
  "unit": "export"
}
```

</details>
