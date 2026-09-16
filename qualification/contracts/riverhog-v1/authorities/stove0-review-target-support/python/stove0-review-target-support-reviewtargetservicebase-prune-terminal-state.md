# stove0_review_target_support.ReviewTargetServiceBase.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-4c8d1fd622:01b25d9a69 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-659ece2799"></a>
- <a id="s-44d751d65f"></a>`distribution`: `stove0-review-target-support`
- <a id="s-a7be729e50"></a>`module`: `stove0_review_target_support`
- <a id="s-658669bc55"></a>`name`: `prune_terminal_state`
- <a id="s-7b369b9e6a"></a>`owner`: `stove0_review_target_support.ReviewTargetServiceBase`
- <a id="s-1fe23a596f"></a>`unit`: `member`

### Declared structure

- <a id="s-f8abd8d199"></a>`kind`: `"method"`
- <a id="s-b2b54c1ccb"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](stove0-review-target-support-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-b2ee8b5ed2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4be845311b6bb9667b3e4d4a114d5da1eb2e25d8450a9119b1d305cde37af950 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "prune_terminal_state",
  "owner": "stove0_review_target_support.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
