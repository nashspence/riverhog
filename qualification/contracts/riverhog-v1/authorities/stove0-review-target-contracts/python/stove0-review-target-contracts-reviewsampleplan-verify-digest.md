# stove0_review_target_contracts.ReviewSamplePlan.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewsamp-481f2040c4:ee10330c9b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bcdc498677"></a>
- <a id="s-d8d45957c2"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-42253b0cbc"></a>`module`: `stove0_review_target_contracts`
- <a id="s-56d159c732"></a>`name`: `verify_digest`
- <a id="s-366e6a4e1c"></a>`owner`: `stove0_review_target_contracts.ReviewSamplePlan`
- <a id="s-fb02c94788"></a>`unit`: `member`

### Declared structure

- <a id="s-b5b0d59ed3"></a>`kind`: `"method"`
- <a id="s-d6b8da6a7c"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplePlan](stove0-review-target-contracts-reviewsampleplan.md)

## Governing policies

- <a id="pa-d303bdc57d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources/authorities.md#src-1d0886e380) — [reference/stove0/targets/review/contracts/src/stove0\_review\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewSamplePlan.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f5a4748961f25e34b9e7f16d6b1acd9e6e34a7e3491b04b16da1e6b7d148e9c7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "verify_digest",
  "owner": "stove0_review_target_contracts.ReviewSamplePlan",
  "unit": "member"
}
```

</details>
