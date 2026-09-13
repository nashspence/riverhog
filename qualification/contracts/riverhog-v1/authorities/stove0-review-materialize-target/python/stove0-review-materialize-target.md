# stove0_review_materialize_target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-materialize-target:stove0-review-materialize-target:5a990c8cf0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1ee35d967d"></a>
| Field | Shape |
|---|---|
| <a id="s-803133ffd6"></a>`candidate_id` | "python:stove0-review-materialize-target:stove0_review_materialize_target" |
| <a id="s-8af787dfd0"></a>`distribution` | "stove0-review-materialize-target" |
| <a id="s-e60926a8e3"></a>`exports` | additional keys=`ReviewMaterializeTargetService` |
| <a id="s-58cb21ac71"></a>`module` | "stove0_review_materialize_target" |

## Governing policies

- <a id="pa-dc13225b48"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-materialize-target:stove0_review_materialize_target](../../../evidence/sources.md#src-d3426939d3) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/__init__.py`

### Machine authority

- `/external_contract/python/49`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35905813452d974fb63ad0046b59f0c85c0b37069ccb818aa294e4060cffdfd0 -->

```json
{
  "candidate_id": "python:stove0-review-materialize-target:stove0_review_materialize_target",
  "distribution": "stove0-review-materialize-target",
  "exports": {
    "ReviewMaterializeTargetService": {
      "kind": "class",
      "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
    }
  },
  "module": "stove0_review_materialize_target"
}
```
