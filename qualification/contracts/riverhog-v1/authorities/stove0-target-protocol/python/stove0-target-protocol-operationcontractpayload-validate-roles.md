# stove0_target_protocol.OperationContractPayload.validate_roles

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-operationcontractp-2a75359f4b:8927fb073f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-249f9ad2bb"></a>
- <a id="s-4663ce2a5e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-c2eee60653"></a>`module`: `stove0_target_protocol`
- <a id="s-6d0fe7e25d"></a>`name`: `validate_roles`
- <a id="s-1d72c3cc23"></a>`owner`: `stove0_target_protocol.OperationContractPayload`
- <a id="s-f544e28909"></a>`unit`: `member`

### Declared structure

- <a id="s-5da9c22737"></a>`kind`: `"method"`
- <a id="s-22aba954f1"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [OperationContractPayload](stove0-target-protocol-operationcontractpayload.md)

## Governing policies

- <a id="pa-c1b7d44827"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OperationContractPayload.validate_roles`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f52161b897b214fea6c90acb4133893214f8bf7cfae6ebb19fabf42d85b1117e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_roles",
  "owner": "stove0_target_protocol.OperationContractPayload",
  "unit": "member"
}
```

</details>
