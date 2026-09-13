# stove0_core.EvaluationReview.meaningful

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationreview-meaningful:698235bbdc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-15fd97048b"></a>
| Field | Shape |
|---|---|
| <a id="s-b4ca9b00fa"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-bbd5a1c728"></a>`distribution` | "stove0-server" |
| <a id="s-e498c31cfe"></a>`module` | "stove0_core" |
| <a id="s-7b22b24a7d"></a>`name` | "meaningful" |
| <a id="s-f5e4eb10e0"></a>`owner` | "stove0_core.EvaluationReview" |
| <a id="s-5568afd1d3"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationReview](stove0-core-evaluationreview.md)

## Governing policies

- <a id="pa-9434cd1ebe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationReview.meaningful`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac5c3fdc3ae1b98ea65699ecdc58bf8b17c34fb19996c3ebe78fd09d513fa6bf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "meaningful",
  "owner": "stove0_core.EvaluationReview",
  "unit": "member"
}
```
