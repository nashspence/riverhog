# stove0_core.WorkStore.record_target_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-record-target-output:a77bd33a07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bbe1083d54"></a>
- <a id="s-66e390014e"></a>`distribution`: `stove0-server`
- <a id="s-25981b0373"></a>`module`: `stove0_core`
- <a id="s-0bb98ce4ee"></a>`name`: `record_target_output`
- <a id="s-e19931ab0f"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-4b9bc27994"></a>`unit`: `member`

### Declared structure

- <a id="s-bd38735776"></a>`kind`: `"method"`
- <a id="s-9801d1bb2c"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-828831fb55"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.record_target_output`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 27c00218fc8e3267e1e6f3bbe359741875ad8116578eefa10ee76aede8a9a2d1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_output",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
