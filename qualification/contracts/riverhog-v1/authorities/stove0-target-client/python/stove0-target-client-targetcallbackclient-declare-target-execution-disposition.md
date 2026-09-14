# stove0_target_client.TargetCallbackClient.declare_target_execution_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetcallbackclient-89ed2a945c:6d5100b8d2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-688ee47ec6"></a>
- <a id="s-61c8df3269"></a>`distribution`: `stove0-target-client`
- <a id="s-2856e5e4e6"></a>`module`: `stove0_target_client`
- <a id="s-7dbad280d2"></a>`name`: `declare_target_execution_disposition`
- <a id="s-0671ba294d"></a>`owner`: `stove0_target_client.TargetCallbackClient`
- <a id="s-9f4ad12086"></a>`unit`: `member`

### Declared structure

- <a id="s-1a48164053"></a>`kind`: `"method"`
- <a id="s-bee220991e"></a>`signature`: `"\"(self, job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'TargetCallbackAcknowledgement'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_client.TargetCallbackClient](stove0-target-client-targetcallbackclient.md)

## Governing policies

- <a id="pa-23a3175c80"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_client.TargetCallbackClient.declare_target_execution_disposition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20e8a09ce630c42fd7c8b3d66d12310a3157dfd4c4fdcdef7d5a5e7c018dd5c4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'TargetCallbackAcknowledgement'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "declare_target_execution_disposition",
  "owner": "stove0_target_client.TargetCallbackClient",
  "unit": "member"
}
```
