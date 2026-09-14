# stove0_target_support.TargetClient.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetclient-put-job:72d137a1ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e5e6cd179e"></a>
- <a id="s-3e96a18b63"></a>`distribution`: `stove0-target-support`
- <a id="s-9301d59ddd"></a>`module`: `stove0_target_support`
- <a id="s-2db44437ce"></a>`name`: `put_job`
- <a id="s-e5b382f236"></a>`owner`: `stove0_target_support.TargetClient`
- <a id="s-d1502ddc13"></a>`unit`: `member`

### Declared structure

- <a id="s-818f7b241c"></a>`kind`: `"method"`
- <a id="s-81d906a910"></a>`signature`: `"\"(self, request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetClient](stove0-target-support-targetclient.md)

## Governing policies

- <a id="pa-a4fcc609c7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetClient.put_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47692143ba4397e019919ae2ad8477fddbf8c747b5680c705d4f7e6f2ad82593 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "put_job",
  "owner": "stove0_target_support.TargetClient",
  "unit": "member"
}
```
