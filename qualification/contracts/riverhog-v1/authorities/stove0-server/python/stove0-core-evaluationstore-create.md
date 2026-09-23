# stove0_core.EvaluationStore.create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationstore-create:1291da6b62 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6176cdf78"></a>
- <a id="s-2395b6da11"></a>`distribution`: `stove0-server`
- <a id="s-946693d462"></a>`module`: `stove0_core`
- <a id="s-339f5b600a"></a>`name`: `create`
- <a id="s-10af7bce06"></a>`owner`: `stove0_core.EvaluationStore`
- <a id="s-5c718617d9"></a>`unit`: `member`

### Declared structure

- <a id="s-e8e0f4d0d5"></a>`kind`: `"method"`
- <a id="s-5a1a803f81"></a>`signature`: `"\"(self, record: 'EvaluationRecord') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationStore](stove0-core-evaluationstore.md)

## Governing policies

- <a id="pa-391a6053ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationStore.create`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91bde2014fd3b026f9484ce319ff1e205d012a034379d6325bbc4ffcd04d56e4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'EvaluationRecord') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create",
  "owner": "stove0_core.EvaluationStore",
  "unit": "member"
}
```

</details>
