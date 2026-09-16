# stove0_core.InMemoryWorkStore.iter_target_source_edges_by_input

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-iter-target-cdd95a17f0:3a89c2466c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-baa2b34c98"></a>
- <a id="s-c5a1fdc106"></a>`distribution`: `stove0-server`
- <a id="s-a62c0c0b8a"></a>`module`: `stove0_core`
- <a id="s-67c10421bb"></a>`name`: `iter_target_source_edges_by_input`
- <a id="s-72cee09bc7"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-e77e362351"></a>`unit`: `member`

### Declared structure

- <a id="s-731fe19109"></a>`kind`: `"method"`
- <a id="s-47aba3d951"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-c111898ed3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.iter_target_source_edges_by_input`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8debcf7e296c92f19914702129014898e7814c7748216836701e3ba54ddf9872 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_source_edges_by_input",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
