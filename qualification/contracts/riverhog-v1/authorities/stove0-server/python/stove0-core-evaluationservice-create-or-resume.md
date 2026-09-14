# stove0_core.EvaluationService.create_or_resume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationservice-create-or-resume:77f3ab89c4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17f9bf28c9"></a>
- <a id="s-9edc53cb02"></a>`distribution`: `stove0-server`
- <a id="s-58ac239006"></a>`module`: `stove0_core`
- <a id="s-7f85a028c6"></a>`name`: `create_or_resume`
- <a id="s-1cee37c83d"></a>`owner`: `stove0_core.EvaluationService`
- <a id="s-dd7e0087df"></a>`unit`: `member`

### Declared structure

- <a id="s-efdc72d17f"></a>`kind`: `"method"`
- <a id="s-927da3c10d"></a>`signature`: `"\"(self, definition: 'EvaluationDefinition') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationService](stove0-core-evaluationservice.md)

## Governing policies

- <a id="pa-dd12883cea"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationService.create_or_resume`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 583592271c363c53a145bb2b8527048e08575022eb25cd1983f0df8cf20a3efd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, definition: 'EvaluationDefinition') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_or_resume",
  "owner": "stove0_core.EvaluationService",
  "unit": "member"
}
```
