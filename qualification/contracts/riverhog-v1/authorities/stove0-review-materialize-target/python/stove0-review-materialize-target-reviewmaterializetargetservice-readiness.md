# stove0_review_materialize_target.ReviewMaterializeTargetService.readiness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target-reviewma-ee83419820:4d1585d9eb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3cbfb67aa4"></a>
- <a id="s-37a36d25d2"></a>`distribution`: `stove0-review-materialize-target`
- <a id="s-2ff6cff751"></a>`module`: `stove0_review_materialize_target`
- <a id="s-bde6c3ae41"></a>`name`: `readiness`
- <a id="s-9c1551e47a"></a>`owner`: `stove0_review_materialize_target.ReviewMaterializeTargetService`
- <a id="s-f23e6c846f"></a>`unit`: `member`

### Declared structure

- <a id="s-d97c0a9a6e"></a>`kind`: `"method"`
- <a id="s-ee020e5044"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](stove0-review-materialize-target-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-4dba1fba2c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources.md#src-d3426939d3) — [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_materialize_target.ReviewMaterializeTargetService.readiness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 202736e21ccb30d8c9a30cf769869d7fcf2ac89be0d876e8c99bc7b943c0f801 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "stove0-review-materialize-target",
  "module": "stove0_review_materialize_target",
  "name": "readiness",
  "owner": "stove0_review_materialize_target.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
