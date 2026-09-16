# stove0_core.WorkStore.iter_target_source_edges_by_input

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-iter-target-source-9adb02ca2e:a28be18320 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-da1b214428"></a>
- <a id="s-1d150132ea"></a>`distribution`: `stove0-server`
- <a id="s-95480fa705"></a>`module`: `stove0_core`
- <a id="s-a23f8b742b"></a>`name`: `iter_target_source_edges_by_input`
- <a id="s-eb46cb02b0"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-1188732049"></a>`unit`: `member`

### Declared structure

- <a id="s-4f9426d346"></a>`kind`: `"method"`
- <a id="s-0a35192edf"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-88b6b49374"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.iter_target_source_edges_by_input`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4e63774f0174671e8a6bceb4333b5d972d0ca6a8cf373abbd5435443d5f75d5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_source_edges_by_input",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
