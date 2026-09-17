# stove0_review_materialize_target.ReviewMaterializeTargetService.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target-reviewma-ea7354811a:f327879788 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e1a257e01"></a>
- <a id="s-47dbe06be3"></a>`distribution`: `stove0-review-materialize-target`
- <a id="s-a2cbead547"></a>`module`: `stove0_review_materialize_target`
- <a id="s-c8bb2abecf"></a>`name`: `prune_terminal_state`
- <a id="s-def090cdd0"></a>`owner`: `stove0_review_materialize_target.ReviewMaterializeTargetService`
- <a id="s-e480acdc19"></a>`unit`: `member`

### Declared structure

- <a id="s-98cef452ef"></a>`kind`: `"method"`
- <a id="s-65713d6369"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](stove0-review-materialize-target-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-f5f718b5b6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources.md#src-d3426939d3) — [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_materialize_target.ReviewMaterializeTargetService.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de1a08ce0bd835650863e23e2c43110df11e25d1e553c7068fa165d54cc932f2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "stove0-review-materialize-target",
  "module": "stove0_review_materialize_target",
  "name": "prune_terminal_state",
  "owner": "stove0_review_materialize_target.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
