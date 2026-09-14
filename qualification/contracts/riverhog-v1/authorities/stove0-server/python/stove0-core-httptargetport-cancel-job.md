# stove0_core.HttpTargetPort.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httptargetport-cancel-job:6382bef032 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-81d9bdd054"></a>
- <a id="s-ab1675da21"></a>`distribution`: `stove0-server`
- <a id="s-c23187a3fb"></a>`module`: `stove0_core`
- <a id="s-ca8fa2776d"></a>`name`: `cancel_job`
- <a id="s-4eb37e3cf8"></a>`owner`: `stove0_core.HttpTargetPort`
- <a id="s-3e1f21e26a"></a>`unit`: `member`

### Declared structure

- <a id="s-cd2a7c86ca"></a>`kind`: `"method"`
- <a id="s-bb501f01be"></a>`signature`: `"\"(self, registration_id: 'str', request: 'TargetJobRequest \| AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.HttpTargetPort](stove0-core-httptargetport.md)

## Governing policies

- <a id="pa-d26ead915a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.HttpTargetPort.cancel_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e4d6d63e8e48da439e91b4687a6105bdb489111172f4c0afa0829a024cbe01b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "cancel_job",
  "owner": "stove0_core.HttpTargetPort",
  "unit": "member"
}
```
