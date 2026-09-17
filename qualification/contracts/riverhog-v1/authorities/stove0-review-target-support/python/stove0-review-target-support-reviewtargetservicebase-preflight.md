# stove0_review_target_support.ReviewTargetServiceBase.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-70f75d6349:60ac3df4f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aaf1d08502"></a>
- <a id="s-756689318f"></a>`distribution`: `stove0-review-target-support`
- <a id="s-7210cd947c"></a>`module`: `stove0_review_target_support`
- <a id="s-cb7f3800fd"></a>`name`: `preflight`
- <a id="s-29c36810e0"></a>`owner`: `stove0_review_target_support.ReviewTargetServiceBase`
- <a id="s-54d0b5fce5"></a>`unit`: `member`

### Declared structure

- <a id="s-df1ed68057"></a>`kind`: `"method"`
- <a id="s-06fd70bed5"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](stove0-review-target-support-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-cad86c335c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources/authorities.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b84ddc1e60cab222d109432a58eddaeb874f11c4eadf7d5d157d2defbca397a7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "preflight",
  "owner": "stove0_review_target_support.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
