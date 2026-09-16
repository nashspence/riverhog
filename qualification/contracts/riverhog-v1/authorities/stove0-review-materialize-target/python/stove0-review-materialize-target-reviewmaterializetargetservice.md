# stove0_review_materialize_target.ReviewMaterializeTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target-reviewma-7cd20a8201:76f82069bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-16283bb630"></a>
- <a id="s-32dc84e17e"></a>`distribution`: `stove0-review-materialize-target`
- <a id="s-cdad2042d8"></a>`module`: `stove0_review_materialize_target`
- <a id="s-d7ec608664"></a>`name`: `ReviewMaterializeTargetService`
- <a id="s-a6d8f2171b"></a>`unit`: `export`

### Declared structure

- <a id="s-83b8cf3dda"></a>`kind`: `"class"`
- <a id="s-49e2d64d54"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [preflight](stove0-review-materialize-target-reviewmaterializetargetservice-preflight.md)
- [get_job](stove0-review-materialize-target-reviewmaterializetargetservice-get-job.md)
- [close](stove0-review-materialize-target-reviewmaterializetargetservice-close.md)
- [cancel_job](stove0-review-materialize-target-reviewmaterializetargetservice-cancel-job.md)
- [put_job](stove0-review-materialize-target-reviewmaterializetargetservice-put-job.md)
- [contract](stove0-review-materialize-target-reviewmaterializetargetservice-contract.md)
- [prune_terminal_state](stove0-review-materialize-target-reviewmaterializetargetservice-prune-terminal-state.md)
- [readiness](stove0-review-materialize-target-reviewmaterializetargetservice-readiness.md)

## Governing policies

- <a id="pa-3137800b3d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources.md#src-d3426939d3) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_materialize_target.ReviewMaterializeTargetService`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25d781377d571768fbc752ece32810f937657002e03523ba2c1dbdec98c31af6 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-review-materialize-target",
  "module": "stove0_review_materialize_target",
  "name": "ReviewMaterializeTargetService",
  "unit": "export"
}
```
