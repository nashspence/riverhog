# stove0_target_client.TargetClient.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetclient-put-job:d52fc78020 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e469f0d28"></a>
- <a id="s-35b24641a9"></a>`distribution`: `stove0-target-client`
- <a id="s-db446929b9"></a>`module`: `stove0_target_client`
- <a id="s-6668a8aefe"></a>`name`: `put_job`
- <a id="s-7ab9b75e58"></a>`owner`: `stove0_target_client.TargetClient`
- <a id="s-b707d12836"></a>`unit`: `member`

### Declared structure

- <a id="s-3b2d952218"></a>`kind`: `"method"`
- <a id="s-19e9cd6c05"></a>`signature`: `"\"(self, request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [TargetClient](stove0-target-client-targetclient.md)

## Governing policies

- <a id="pa-768887fe4f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources/authorities.md#src-be4c80156f) — [reference/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetClient.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c28affec07b3348164ed372bb5379aa4d55bcb2b0c7dbc4fe6e719f49d1633be -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "put_job",
  "owner": "stove0_target_client.TargetClient",
  "unit": "member"
}
```

</details>
