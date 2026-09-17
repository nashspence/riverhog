# stove0_review_materialize_target.ReviewMaterializeTargetService.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target-reviewma-bd3b5cf1af:ecd406452a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ebe28dc5c9"></a>
- <a id="s-2e1149f4ab"></a>`distribution`: `stove0-review-materialize-target`
- <a id="s-5ecace247c"></a>`module`: `stove0_review_materialize_target`
- <a id="s-4ec5c3ceee"></a>`name`: `put_job`
- <a id="s-67b098c403"></a>`owner`: `stove0_review_materialize_target.ReviewMaterializeTargetService`
- <a id="s-64de447a4b"></a>`unit`: `member`

### Declared structure

- <a id="s-2ff5b2c304"></a>`kind`: `"method"`
- <a id="s-9ef7adda40"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](stove0-review-materialize-target-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-14744e904b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources/authorities.md#src-d3426939d3) — [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_materialize_target.ReviewMaterializeTargetService.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be256b6f30eca1d4bb34ae52d1f8254ba0f8f9ccd8494a6e45937a82eb990cd1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-materialize-target",
  "module": "stove0_review_materialize_target",
  "name": "put_job",
  "owner": "stove0_review_materialize_target.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
