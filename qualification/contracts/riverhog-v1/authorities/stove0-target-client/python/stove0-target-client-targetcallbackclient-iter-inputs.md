# stove0_target_client.TargetCallbackClient.iter_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetcallbackclient-7228226f2e:f78e8e8a8f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-81683ba1a1"></a>
- <a id="s-585c7465b8"></a>`distribution`: `stove0-target-client`
- <a id="s-48a0247c6c"></a>`module`: `stove0_target_client`
- <a id="s-b5a91337dc"></a>`name`: `iter_inputs`
- <a id="s-ab5001db6b"></a>`owner`: `stove0_target_client.TargetCallbackClient`
- <a id="s-9123fb20ce"></a>`unit`: `member`

### Declared structure

- <a id="s-323114d431"></a>`kind`: `"method"`
- <a id="s-b5ad987a43"></a>`signature`: `"\"(self, job_id: 'str') -> 'Iterator[InputArtifact]'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackClient](stove0-target-client-targetcallbackclient.md)

## Governing policies

- <a id="pa-176a6dd252"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — [reference/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetCallbackClient.iter_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f15b3c3b156e6ec8d7c4719d814acc23991bc88f06bf079a5ca548d9ac3ca46 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'Iterator[InputArtifact]'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "iter_inputs",
  "owner": "stove0_target_client.TargetCallbackClient",
  "unit": "member"
}
```

</details>
