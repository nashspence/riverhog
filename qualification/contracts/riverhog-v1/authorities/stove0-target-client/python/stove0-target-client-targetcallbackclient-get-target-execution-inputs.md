# stove0_target_client.TargetCallbackClient.get_target_execution_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetcallbackclient-a3c174da4f:6752491b58 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db1ef58fe2"></a>
- <a id="s-9b61b0b679"></a>`distribution`: `stove0-target-client`
- <a id="s-040ac06413"></a>`module`: `stove0_target_client`
- <a id="s-53db9e964e"></a>`name`: `get_target_execution_inputs`
- <a id="s-1a04ead143"></a>`owner`: `stove0_target_client.TargetCallbackClient`
- <a id="s-a112bbed18"></a>`unit`: `member`

### Declared structure

- <a id="s-3cfb514a65"></a>`kind`: `"method"`
- <a id="s-90784e6b05"></a>`signature`: `"\"(self, job_id: 'str', *, continuation: 'str \| None' = None) -> 'TargetInputPage'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/target-executions/{job_id}/inputs](../../stove0/http-operations/get-v1-target-executions-job-id-inputs.md)
- [TargetCallbackClient](stove0-target-client-targetcallbackclient.md)

## Governing policies

- <a id="pa-6d746b63f4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`
- **Client method:** [reference/stove0/packages/target-client/src/stove0_target_client/client.py::TargetCallbackClient.get_target_execution_inputs](../../../../../../reference/stove0/packages/target-client/src/stove0_target_client/client.py#L221)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetCallbackClient.get_target_execution_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 65ef6a050a7ae3506481aeac1f7462291e7f6e8a804857e9159553781fa499a9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, continuation: 'str | None' = None) -> 'TargetInputPage'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "get_target_execution_inputs",
  "owner": "stove0_target_client.TargetCallbackClient",
  "unit": "member"
}
```

</details>
