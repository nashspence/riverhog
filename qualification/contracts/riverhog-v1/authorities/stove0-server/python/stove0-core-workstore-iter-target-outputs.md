# stove0_core.WorkStore.iter_target_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-iter-target-outputs:d4a8a3f1be -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-787d8d0d26"></a>
- <a id="s-325b0db8a6"></a>`distribution`: `stove0-server`
- <a id="s-bba1f2b778"></a>`module`: `stove0_core`
- <a id="s-bb47dd9f0d"></a>`name`: `iter_target_outputs`
- <a id="s-1b736769ff"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-5f2abe4a5c"></a>`unit`: `member`

### Declared structure

- <a id="s-42bf49ad6c"></a>`kind`: `"method"`
- <a id="s-89efc7f6dd"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-53c1510983"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.iter_target_outputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc48f86bcc89240e6e3ebb05862da182efd643adaab59546aea5a2e67f998159 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_outputs",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
