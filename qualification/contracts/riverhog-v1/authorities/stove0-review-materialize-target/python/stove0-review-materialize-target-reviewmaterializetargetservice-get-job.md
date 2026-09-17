# stove0_review_materialize_target.ReviewMaterializeTargetService.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target-reviewma-97140db4ec:167bae9792 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b2e3d98452"></a>
- <a id="s-b10b21fc7c"></a>`distribution`: `stove0-review-materialize-target`
- <a id="s-43297bc465"></a>`module`: `stove0_review_materialize_target`
- <a id="s-4d0854589a"></a>`name`: `get_job`
- <a id="s-40be8ca73b"></a>`owner`: `stove0_review_materialize_target.ReviewMaterializeTargetService`
- <a id="s-47875519d9"></a>`unit`: `member`

### Declared structure

- <a id="s-0cf86398db"></a>`kind`: `"method"`
- <a id="s-ed3a349a48"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](stove0-review-materialize-target-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-b32306e7d0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources/authorities.md#src-d3426939d3) — [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_materialize_target.ReviewMaterializeTargetService.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92186b0a41c8a7a6a86a8eff85f14f2acd06f518230ba6b6c7d94c373220dd9f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-materialize-target",
  "module": "stove0_review_materialize_target",
  "name": "get_job",
  "owner": "stove0_review_materialize_target.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
