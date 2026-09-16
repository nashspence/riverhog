# stove0_review_materialize_target.ReviewMaterializeTargetService.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target-reviewma-b5bcc27348:1db9b88399 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-746def75be"></a>
- <a id="s-f0b68d7e64"></a>`distribution`: `stove0-review-materialize-target`
- <a id="s-0dd0a976c5"></a>`module`: `stove0_review_materialize_target`
- <a id="s-d5e7324e20"></a>`name`: `cancel_job`
- <a id="s-6b498c0e2b"></a>`owner`: `stove0_review_materialize_target.ReviewMaterializeTargetService`
- <a id="s-cb7408b45c"></a>`unit`: `member`

### Declared structure

- <a id="s-4fe34fb3e4"></a>`kind`: `"method"`
- <a id="s-b71ff1cbed"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](stove0-review-materialize-target-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-76768850e0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources.md#src-d3426939d3) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_materialize_target.ReviewMaterializeTargetService.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec09f07fdf74bdc057e24aca9cdc6e6935ebbd3b673196930137562f4d88fef2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-materialize-target",
  "module": "stove0_review_materialize_target",
  "name": "cancel_job",
  "owner": "stove0_review_materialize_target.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
