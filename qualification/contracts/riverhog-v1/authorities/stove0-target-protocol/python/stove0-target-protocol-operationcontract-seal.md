# stove0_target_protocol.OperationContract.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-operationcontract-seal:9fde8f3feb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bb59194204"></a>
- <a id="s-ee987af484"></a>`distribution`: `stove0-target-protocol`
- <a id="s-6d3f059056"></a>`module`: `stove0_target_protocol`
- <a id="s-11da6e7198"></a>`name`: `seal`
- <a id="s-c215dd80ee"></a>`owner`: `stove0_target_protocol.OperationContract`
- <a id="s-38e427c1c9"></a>`unit`: `member`

### Declared structure

- <a id="s-9ddaf375ed"></a>`kind`: `"classmethod"`
- <a id="s-3678495105"></a>`signature`: `"\"(cls, payload: 'OperationContractPayload') -> 'OperationContract'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OperationContract](stove0-target-protocol-operationcontract.md)

## Governing policies

- <a id="pa-67cbd9dfee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OperationContract.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4674f99295a8a67a240dd31c064e249d49cac244edc1f544abb7402a27eea117 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'OperationContractPayload') -> 'OperationContract'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.OperationContract",
  "unit": "member"
}
```
