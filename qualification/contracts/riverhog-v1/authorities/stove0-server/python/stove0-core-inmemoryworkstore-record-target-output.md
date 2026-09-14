# stove0_core.InMemoryWorkStore.record_target_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-record-target-output:32b012d64c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1d61b33bce"></a>
- <a id="s-434230de78"></a>`distribution`: `stove0-server`
- <a id="s-437a3e4880"></a>`module`: `stove0_core`
- <a id="s-1cda631f44"></a>`name`: `record_target_output`
- <a id="s-1aafc73d2d"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-55b99b8df5"></a>`unit`: `member`

### Declared structure

- <a id="s-8e91f8fb89"></a>`kind`: `"method"`
- <a id="s-dbbef4bd53"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-b969b3821b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.record_target_output`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa7b8b352afafafc6456dcbdffa89157e880b5de291878d26001f22f4ba231d2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_output",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
