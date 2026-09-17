# stove0_target_client.TargetCallbackClient.declare_target_execution_source_edge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetcallbackclient-4b86ca307b:c355cdc462 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-073bd7d516"></a>
- <a id="s-041135df96"></a>`distribution`: `stove0-target-client`
- <a id="s-2b80b36be0"></a>`module`: `stove0_target_client`
- <a id="s-68dc1f1559"></a>`name`: `declare_target_execution_source_edge`
- <a id="s-8c72af54c8"></a>`owner`: `stove0_target_client.TargetCallbackClient`
- <a id="s-57fbb386f3"></a>`unit`: `member`

### Declared structure

- <a id="s-76b62d6e56"></a>`kind`: `"method"`
- <a id="s-f5c1427062"></a>`signature`: `"\"(self, job_id: 'str', edge: 'OutputSourceEdge') -> 'TargetCallbackAcknowledgement'\""`

## Maintained corroboration

### Related interface records

- [PUT /v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}](../../stove0/http-operations/put-v1-target-executions-job-id-source-edges-output-id-input-id.md)
- [TargetCallbackClient](stove0-target-client-targetcallbackclient.md)

## Governing policies

- <a id="pa-8e14558f05"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — [reference/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-client/src/stove0_target_client/__init__.py)
- **Client method:** [reference/stove0/packages/target-client/src/stove0\_target\_client/client.py::TargetCallbackClient.declare\_target\_execution\_source\_edge](../../../../../../reference/stove0/packages/target-client/src/stove0_target_client/client.py#L255)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetCallbackClient.declare_target_execution_source_edge`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d014a83d9ac9c51b572c20bf11460346c74de27aecd729e14bde0be51074515f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', edge: 'OutputSourceEdge') -> 'TargetCallbackAcknowledgement'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "declare_target_execution_source_edge",
  "owner": "stove0_target_client.TargetCallbackClient",
  "unit": "member"
}
```

</details>
