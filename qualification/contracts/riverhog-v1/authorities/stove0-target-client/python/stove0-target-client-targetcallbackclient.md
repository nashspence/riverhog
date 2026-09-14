# stove0_target_client.TargetCallbackClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetcallbackclient:ed3d38594c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0a34942177"></a>
- <a id="s-b71ee9bf17"></a>`distribution`: `stove0-target-client`
- <a id="s-000ae12133"></a>`module`: `stove0_target_client`
- <a id="s-ed42595704"></a>`name`: `TargetCallbackClient`
- <a id="s-d232870af5"></a>`unit`: `export`

### Declared structure

- <a id="s-385bb00ddd"></a>`kind`: `"class"`
- <a id="s-01f202b8ea"></a>`signature`: `"\"(access: 'TargetCallbackAccess', *, timeout: 'float \| None' = 300.0) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_client.TargetCallbackClient.seal_target_execution_production](stove0-target-client-targetcallbackclient-seal-target-execution-production.md)
- [stove0_target_client.TargetCallbackClient.declare_target_execution_source_edge](stove0-target-client-targetcallbackclient-declare-target-execution-source-edge.md)
- [stove0_target_client.TargetCallbackClient.iter_inputs](stove0-target-client-targetcallbackclient-iter-inputs.md)
- [stove0_target_client.TargetCallbackClient.declare_target_execution_disposition](stove0-target-client-targetcallbackclient-declare-target-execution-disposition.md)
- [stove0_target_client.TargetCallbackClient.get_target_execution_inputs](stove0-target-client-targetcallbackclient-get-target-execution-inputs.md)
- [stove0_target_client.TargetCallbackClient.declare_target_execution_output](stove0-target-client-targetcallbackclient-declare-target-execution-output.md)
- [stove0_target_client.TargetCallbackClient.close](stove0-target-client-targetcallbackclient-close.md)

## Governing policies

- <a id="pa-13f962cb1c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_client.TargetCallbackClient`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcb7c52b0a833133b24f7afc988b4770f67f3316f0ffccfc5cb424fc8c00a16f -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(access: 'TargetCallbackAccess', *, timeout: 'float | None' = 300.0) -> 'None'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "TargetCallbackClient",
  "unit": "export"
}
```
