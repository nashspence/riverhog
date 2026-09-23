# stove0_target_client.TargetClient.cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetclient-cancel:9a8118d710 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75e2abc7af"></a>
- <a id="s-aec9febef4"></a>`distribution`: `stove0-target-client`
- <a id="s-82a474e0d1"></a>`module`: `stove0_target_client`
- <a id="s-3ea5f441c3"></a>`name`: `cancel`
- <a id="s-1e8c26ca41"></a>`owner`: `stove0_target_client.TargetClient`
- <a id="s-4c206d6a0a"></a>`unit`: `member`

### Declared structure

- <a id="s-dc6a3afda2"></a>`kind`: `"method"`
- <a id="s-9299dd9817"></a>`signature`: `"\"(self, request: 'TargetJobRequest \| AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [TargetClient](stove0-target-client-targetclient.md)

## Governing policies

- <a id="pa-40ee467fc7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources/authorities.md#src-be4c80156f) — [some-implementations/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetClient.cancel`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0ce6561aa03ca0e3d9301e7a632653d92de86d63ca2776c7b84f48b91b15510 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "cancel",
  "owner": "stove0_target_client.TargetClient",
  "unit": "member"
}
```

</details>
