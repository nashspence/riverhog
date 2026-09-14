# stove0_core.WorkStore.compare_and_swap

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-compare-and-swap:f8d503f93f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bad43314c1"></a>
- <a id="s-9d18a711a3"></a>`distribution`: `stove0-server`
- <a id="s-53302b5378"></a>`module`: `stove0_core`
- <a id="s-fcc44690f4"></a>`name`: `compare_and_swap`
- <a id="s-5af4dc0c75"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-1c730aa6d2"></a>`unit`: `member`

### Declared structure

- <a id="s-562d20a6a1"></a>`kind`: `"method"`
- <a id="s-196423754a"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int', replacement: 'WorkRecord') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-d29bdd2e68"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.compare_and_swap`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d01a12393d7b78e47278b9b62f8220f3fd54100404035bda75e471bfb57f4b41 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', replacement: 'WorkRecord') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
