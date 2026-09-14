# stove0_core.InMemoryWorkStore.load_target_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load-target-output:0830f11fa4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f046742e18"></a>
- <a id="s-5a6d0036c5"></a>`distribution`: `stove0-server`
- <a id="s-1126c9c048"></a>`module`: `stove0_core`
- <a id="s-41f3dbbb3e"></a>`name`: `load_target_output`
- <a id="s-989c5174fe"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-411540259c"></a>`unit`: `member`

### Declared structure

- <a id="s-1964ed5418"></a>`kind`: `"method"`
- <a id="s-8017a68745"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-f8d4628c67"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load_target_output`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbbe531fee88c0558e512da82124a50a6502bc7c21c6d2f539580cd0f670067c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_output",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
