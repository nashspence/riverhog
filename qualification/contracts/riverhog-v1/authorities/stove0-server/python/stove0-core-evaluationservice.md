# stove0_core.EvaluationService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationservice:144d4a166e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75581c9ed5"></a>
- <a id="s-9737180487"></a>`distribution`: `stove0-server`
- <a id="s-6696d063b5"></a>`module`: `stove0_core`
- <a id="s-79dca759ca"></a>`name`: `EvaluationService`
- <a id="s-af03b5b1d0"></a>`unit`: `export`

### Declared structure

- <a id="s-f7c1cc7a52"></a>`kind`: `"class"`
- <a id="s-8c8234a4f3"></a>`signature`: `"\"(store: 'EvaluationStore', *, work: 'Stove0WorkService') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [cancel](stove0-core-evaluationservice-cancel.md)
- [create_or_resume](stove0-core-evaluationservice-create-or-resume.md)
- [refresh](stove0-core-evaluationservice-refresh.md)
- [retry_failed](stove0-core-evaluationservice-retry-failed.md)
- [review](stove0-core-evaluationservice-review.md)
- [step](stove0-core-evaluationservice-step.md)

## Governing policies

- <a id="pa-6fe399f73f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84341e243277441efb0b9fba764a1bfa89dfbde905b265ec76cafd0b46d0e07a -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(store: 'EvaluationStore', *, work: 'Stove0WorkService') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EvaluationService",
  "unit": "export"
}
```

</details>
