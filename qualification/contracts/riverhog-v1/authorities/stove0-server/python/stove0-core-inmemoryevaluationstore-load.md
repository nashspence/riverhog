# stove0_core.InMemoryEvaluationStore.load

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryevaluationstore-load:92822bd2f6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-38a046e904"></a>
- <a id="s-26d57a1890"></a>`distribution`: `stove0-server`
- <a id="s-ed550220b3"></a>`module`: `stove0_core`
- <a id="s-9d826d63c1"></a>`name`: `load`
- <a id="s-1da86033d7"></a>`owner`: `stove0_core.InMemoryEvaluationStore`
- <a id="s-0068de29d0"></a>`unit`: `member`

### Declared structure

- <a id="s-269f5b432a"></a>`kind`: `"method"`
- <a id="s-83d16daf30"></a>`signature`: `"\"(self, evaluation_id: 'str') -> 'EvaluationRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryEvaluationStore](stove0-core-inmemoryevaluationstore.md)

## Governing policies

- <a id="pa-7b0fa8c4ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryEvaluationStore.load`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e659f37bb7d79279cd14f3762febb21ea56cefd61dc75e93ac2152a14d2dcce -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load",
  "owner": "stove0_core.InMemoryEvaluationStore",
  "unit": "member"
}
```
