# stove0_target_protocol.validate_status_against_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-validate-status-ag-baa0b41196:7e1c9c09fb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b91816e16"></a>
- <a id="s-c404cb69e8"></a>`distribution`: `stove0-target-protocol`
- <a id="s-ca0562b22a"></a>`module`: `stove0_target_protocol`
- <a id="s-0177889178"></a>`name`: `validate_status_against_request`
- <a id="s-1f2fe1a3d2"></a>`unit`: `export`

### Declared structure

- <a id="s-61a07927eb"></a>`kind`: `"function"`
- <a id="s-4b7accd488"></a>`signature`: `"\"(status: 'TargetJobStatus', request: 'TargetJobRequest \| AcceptedTargetJob', operation: 'OperationContract') -> 'None'\""`

## Governing policies

- <a id="pa-15a3a3b367"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.validate_status_against_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d13e813c6c30cb35221cee76aeafc66dcf677957406fb388214bc0955e6b4c4e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(status: 'TargetJobStatus', request: 'TargetJobRequest | AcceptedTargetJob', operation: 'OperationContract') -> 'None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_status_against_request",
  "unit": "export"
}
```

</details>
