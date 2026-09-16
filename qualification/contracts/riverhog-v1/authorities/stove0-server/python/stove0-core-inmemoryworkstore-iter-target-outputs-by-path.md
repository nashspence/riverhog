# stove0_core.InMemoryWorkStore.iter_target_outputs_by_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-iter-target-e51160df40:14a3cd68f4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d79bd988e"></a>
- <a id="s-14e85247c7"></a>`distribution`: `stove0-server`
- <a id="s-6b509a0e64"></a>`module`: `stove0_core`
- <a id="s-123c97068d"></a>`name`: `iter_target_outputs_by_path`
- <a id="s-14c15e90a5"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-c219e04511"></a>`unit`: `member`

### Declared structure

- <a id="s-e862319eda"></a>`kind`: `"method"`
- <a id="s-cef2e8bce7"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-3940c450e7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.iter_target_outputs_by_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d9d43d47da13c6b45c18190432835be669ceba30895071f87a1bb49378529f3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_outputs_by_path",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
