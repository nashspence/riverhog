# stove0_review_materialize_target.ReviewMaterializeTargetService.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target-reviewma-72e454d78c:8da4e0f627 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5702e3b1e2"></a>
- <a id="s-b0e9a2332b"></a>`distribution`: `stove0-review-materialize-target`
- <a id="s-603a751c46"></a>`module`: `stove0_review_materialize_target`
- <a id="s-f0ac593868"></a>`name`: `preflight`
- <a id="s-8812040af9"></a>`owner`: `stove0_review_materialize_target.ReviewMaterializeTargetService`
- <a id="s-720f43d0ba"></a>`unit`: `member`

### Declared structure

- <a id="s-3e631bc073"></a>`kind`: `"method"`
- <a id="s-a215e41e16"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](stove0-review-materialize-target-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-aa186686a1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources.md#src-d3426939d3) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_materialize_target.ReviewMaterializeTargetService.preflight`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cea717e2a69b5333dade4798145bf0efb9ef45bbea4376e51d1a67dc8fd969cc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-review-materialize-target",
  "module": "stove0_review_materialize_target",
  "name": "preflight",
  "owner": "stove0_review_materialize_target.ReviewMaterializeTargetService",
  "unit": "member"
}
```
