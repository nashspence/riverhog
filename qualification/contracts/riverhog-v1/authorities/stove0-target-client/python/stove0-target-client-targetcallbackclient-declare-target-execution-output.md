# stove0_target_client.TargetCallbackClient.declare_target_execution_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetcallbackclient-bf97d61403:d93fddb1ed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-00a00d4a94"></a>
| Field | Shape |
|---|---|
| <a id="s-1402ef7676"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b4d16ea98d"></a>`distribution` | "stove0-target-client" |
| <a id="s-5f088afc23"></a>`module` | "stove0_target_client" |
| <a id="s-4ff4f2a0ef"></a>`name` | "declare_target_execution_output" |
| <a id="s-3923715c52"></a>`owner` | "stove0_target_client.TargetCallbackClient" |
| <a id="s-0e99e2a16f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_client.TargetCallbackClient](stove0-target-client-targetcallbackclient.md)

## Governing policies

- <a id="pa-55710498e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_client.TargetCallbackClient.declare_target_execution_output`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b1a17a7ea21afd01a90a919519c2fdea4f0a648948772463ec5aa32093771ce -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', output: 'OutputArtifact') -> 'TargetCallbackAcknowledgement'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "declare_target_execution_output",
  "owner": "stove0_target_client.TargetCallbackClient",
  "unit": "member"
}
```
