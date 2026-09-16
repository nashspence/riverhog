# stove0_target_client.TargetCallbackClient.seal_target_execution_production

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetcallbackclient-0dccba5377:1c772dd82f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a988c4a0d6"></a>
- <a id="s-25e2fb4aad"></a>`distribution`: `stove0-target-client`
- <a id="s-060ff6fbf9"></a>`module`: `stove0_target_client`
- <a id="s-ae538a06f3"></a>`name`: `seal_target_execution_production`
- <a id="s-798a0ae11f"></a>`owner`: `stove0_target_client.TargetCallbackClient`
- <a id="s-1fcc5a6b1a"></a>`unit`: `member`

### Declared structure

- <a id="s-3487fb17b3"></a>`kind`: `"method"`
- <a id="s-00747f1a7f"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetProductionSealResponse'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/target-executions/{job_id}/production/seal](../../stove0/http-operations/post-v1-target-executions-job-id-production-seal.md)
- [TargetCallbackClient](stove0-target-client-targetcallbackclient.md)

## Governing policies

- <a id="pa-d80d61fc46"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`
- **Client method:** [reference/stove0/packages/target-client/src/stove0_target_client/client.py::TargetCallbackClient.seal_target_execution_production](../../../../../../reference/stove0/packages/target-client/src/stove0_target_client/client.py#L265)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetCallbackClient.seal_target_execution_production`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 69df5c937956c49008b0e054c798fb2c5277a418f381fde7fad40b18c58c91ad -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetProductionSealResponse'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "seal_target_execution_production",
  "owner": "stove0_target_client.TargetCallbackClient",
  "unit": "member"
}
```

</details>
