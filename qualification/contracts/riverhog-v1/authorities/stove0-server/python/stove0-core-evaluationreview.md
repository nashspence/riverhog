# stove0_core.EvaluationReview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationreview:a878834366 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a1d072f335"></a>
| Field | Shape |
|---|---|
| <a id="s-37a4543d63"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d6ef84d860"></a>`distribution` | "stove0-server" |
| <a id="s-2bd2b6be8a"></a>`module` | "stove0_core" |
| <a id="s-ebf4a78d89"></a>`name` | "EvaluationReview" |
| <a id="s-ee558774f9"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationReview.meaningful](stove0-core-evaluationreview-meaningful.md)

## Governing policies

- <a id="pa-52a3cc2577"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationReview`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1801e3efe1daec985858eb9bb34146ee903aebda619f98940aa1f5f66c6967c6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c7ca0c39feb6bf6039a1307a6d0b77476adef3c5968e2de4fba9e0a9c646332c",
    "signature": "'(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[str | None, MaxLen(max_length=4000)] = None, updated_by: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EvaluationReview",
  "unit": "export"
}
```
