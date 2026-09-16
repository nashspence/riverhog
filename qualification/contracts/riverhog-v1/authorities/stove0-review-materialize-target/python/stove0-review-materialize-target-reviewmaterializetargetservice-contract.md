# stove0_review_materialize_target.ReviewMaterializeTargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target-reviewma-cc8f2583be:cc66f5b3ed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d99f91f29f"></a>
- <a id="s-f65d4362a0"></a>`distribution`: `stove0-review-materialize-target`
- <a id="s-29684118ec"></a>`module`: `stove0_review_materialize_target`
- <a id="s-d7e323673c"></a>`name`: `contract`
- <a id="s-2f38e3b60d"></a>`owner`: `stove0_review_materialize_target.ReviewMaterializeTargetService`
- <a id="s-0858857465"></a>`unit`: `member`

### Declared structure

- <a id="s-2cc80fe0ec"></a>`kind`: `"method"`
- <a id="s-e1bf229be7"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](stove0-review-materialize-target-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-e255bd2dda"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources.md#src-d3426939d3) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_materialize_target.ReviewMaterializeTargetService.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2ec28d28c029590169abf013870301026b2e93315ef9457cd2f7683e1502ba4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "stove0-review-materialize-target",
  "module": "stove0_review_materialize_target",
  "name": "contract",
  "owner": "stove0_review_materialize_target.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
