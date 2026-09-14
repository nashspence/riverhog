# stove0_core.InMemoryWorkStore.load_target_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load-target-4e960c1ac2:c65aa6cebe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8639bfee79"></a>
- <a id="s-69ff0ac001"></a>`distribution`: `stove0-server`
- <a id="s-06f5f9e010"></a>`module`: `stove0_core`
- <a id="s-615d1f6976"></a>`name`: `load_target_disposition`
- <a id="s-927bb015bb"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-7a0d283084"></a>`unit`: `member`

### Declared structure

- <a id="s-d33d6de7cf"></a>`kind`: `"method"`
- <a id="s-212b005eb9"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration \| None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-0d6d8b3ca5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load_target_disposition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 378e398a5cebcfb64c9820904e6a596b401fe347834f3528233bd516867d544a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_disposition",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
