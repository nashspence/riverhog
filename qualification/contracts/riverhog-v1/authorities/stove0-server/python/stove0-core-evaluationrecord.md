# stove0_core.EvaluationRecord

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationrecord:51c18da39c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-400e8f7ffa"></a>
| Field | Shape |
|---|---|
| <a id="s-21aa8154a3"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7fc4e7849b"></a>`distribution` | "stove0-server" |
| <a id="s-208f491553"></a>`module` | "stove0_core" |
| <a id="s-2a1986573e"></a>`name` | "EvaluationRecord" |
| <a id="s-7aed46a79d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationRecord.evaluation_id](stove0-core-evaluationrecord-evaluation-id.md)
- [stove0_core.EvaluationRecord.validate_children](stove0-core-evaluationrecord-validate-children.md)

## Governing policies

- <a id="pa-c9bf4a1f4f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationRecord`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 884227b9244c47a6e065afb306f180191cd50caa03beaec33e2f7430e1ef4cb4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ab8b675f9648345599ff6e54ee4ad1a358dcd426ae98766419ced7bb2b78339d",
    "signature": "\"(*, format: Literal['stove0-evaluation-record/v1'] = 'stove0-evaluation-record/v1', definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'] = 'planning', revision: Annotated[int, Ge(ge=1)] = 1, children: tuple[stove0_core.evaluation.EvaluationChild, ...], reviews: tuple[stove0_core.evaluation.EvaluationReview, ...] = ()) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EvaluationRecord",
  "unit": "export"
}
```
