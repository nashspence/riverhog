# stove0_target_protocol.TargetContractPayload.bind_result_kind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcontractpayl-5306404a24:c079f6d3c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e33f23cde8"></a>
- <a id="s-1306b8b361"></a>`distribution`: `stove0-target-protocol`
- <a id="s-a12aada689"></a>`module`: `stove0_target_protocol`
- <a id="s-748c8fef50"></a>`name`: `bind_result_kind`
- <a id="s-a348233def"></a>`owner`: `stove0_target_protocol.TargetContractPayload`
- <a id="s-0e53b6d855"></a>`unit`: `member`

### Declared structure

- <a id="s-8e3fc90a91"></a>`kind`: `"method"`
- <a id="s-f5d49c82c6"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetContractPayload](stove0-target-protocol-targetcontractpayload.md)

## Governing policies

- <a id="pa-2492e15298"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetContractPayload.bind_result_kind`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c265d919eec04b7916f04dbedeab4e779f35b14f283414572d4a1339bccf82e9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "bind_result_kind",
  "owner": "stove0_target_protocol.TargetContractPayload",
  "unit": "member"
}
```
