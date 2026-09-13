# stove0_target_client.TargetClient.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetclient-status:b71f684878 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24f5276af6"></a>
| Field | Shape |
|---|---|
| <a id="s-0620e0d2b1"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ccbf3a3fbd"></a>`distribution` | "stove0-target-client" |
| <a id="s-a66c3f3ad0"></a>`module` | "stove0_target_client" |
| <a id="s-29126d36ae"></a>`name` | "status" |
| <a id="s-e2b95f086b"></a>`owner` | "stove0_target_client.TargetClient" |
| <a id="s-d35d1db8ee"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_client.TargetClient](stove0-target-client-targetclient.md)

## Governing policies

- <a id="pa-26964e0bee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_client.TargetClient.status`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6444843d21c73160def998cf9b4b504b5156b9e4b4af3e0a3518e07457b32d6e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "status",
  "owner": "stove0_target_client.TargetClient",
  "unit": "member"
}
```
