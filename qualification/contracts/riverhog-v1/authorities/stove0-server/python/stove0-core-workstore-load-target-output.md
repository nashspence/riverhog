# stove0_core.WorkStore.load_target_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-load-target-output:242a2f3f82 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e487fb610b"></a>
- <a id="s-efa1246c90"></a>`distribution`: `stove0-server`
- <a id="s-6968ad7410"></a>`module`: `stove0_core`
- <a id="s-5410159027"></a>`name`: `load_target_output`
- <a id="s-95d0cb38cd"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-66b77ec8bc"></a>`unit`: `member`

### Declared structure

- <a id="s-183312d40c"></a>`kind`: `"method"`
- <a id="s-e2af1102db"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact \| None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-927506fbf6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.load_target_output`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb88151807ecb1bcaa8d470f19cdd96f1649c2aa8e7b3da5a858d267e5f8efae -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_output",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
