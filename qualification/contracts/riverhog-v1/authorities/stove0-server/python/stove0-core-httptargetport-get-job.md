# stove0_core.HttpTargetPort.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httptargetport-get-job:fa8800bdb8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-329a567965"></a>
- <a id="s-88bd7bec12"></a>`distribution`: `stove0-server`
- <a id="s-49b9246949"></a>`module`: `stove0_core`
- <a id="s-2ab349cef7"></a>`name`: `get_job`
- <a id="s-2b78f0b750"></a>`owner`: `stove0_core.HttpTargetPort`
- <a id="s-cd4802f243"></a>`unit`: `member`

### Declared structure

- <a id="s-23d31aa493"></a>`kind`: `"method"`
- <a id="s-539de30a08"></a>`signature`: `"\"(self, registration_id: 'str', request: 'TargetJobRequest \| AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [HttpTargetPort](stove0-core-httptargetport.md)

## Governing policies

- <a id="pa-1f7f1f0730"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.HttpTargetPort.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52b9d2e0aa34c4aff924ad5ea1685a2cd3b201c96ccd2b69f43969b8adf9c83d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_job",
  "owner": "stove0_core.HttpTargetPort",
  "unit": "member"
}
```

</details>
