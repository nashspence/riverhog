# stove0_core.EvaluationStore.compare_and_swap

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationstore-compare-and-swap:a969b83163 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-33ec4247d1"></a>
- <a id="s-c7dece2aaf"></a>`distribution`: `stove0-server`
- <a id="s-c267e73847"></a>`module`: `stove0_core`
- <a id="s-49fa349a8c"></a>`name`: `compare_and_swap`
- <a id="s-39a16e80be"></a>`owner`: `stove0_core.EvaluationStore`
- <a id="s-e828288ace"></a>`unit`: `member`

### Declared structure

- <a id="s-05edc3c55e"></a>`kind`: `"method"`
- <a id="s-0861db4eaf"></a>`signature`: `"\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationStore](stove0-core-evaluationstore.md)

## Governing policies

- <a id="pa-3e6de86eb5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationStore.compare_and_swap`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9483ecc2e3b0580b346da6b803207cda3e4b774e2c9f7ac9427e0393f3d9e44d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap",
  "owner": "stove0_core.EvaluationStore",
  "unit": "member"
}
```

</details>
