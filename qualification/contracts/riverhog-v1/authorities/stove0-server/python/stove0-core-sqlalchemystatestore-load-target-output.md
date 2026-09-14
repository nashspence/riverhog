# stove0_core.SqlAlchemyStateStore.load_target_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-target-output:2387840d3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6bba327398"></a>
- <a id="s-26162a82a0"></a>`distribution`: `stove0-server`
- <a id="s-b2920acddf"></a>`module`: `stove0_core`
- <a id="s-6a9ca6c357"></a>`name`: `load_target_output`
- <a id="s-91229ee443"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-6f250f4969"></a>`unit`: `member`

### Declared structure

- <a id="s-2987e0f061"></a>`kind`: `"method"`
- <a id="s-944bbcea00"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-55637e6345"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_target_output`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95e235e4d9ba0f4b569187e8d80a01086d188a3b8d6d55c7877d66e19ba3441e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_output",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
