# stove0_protocol.OperationRef.to_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-operationref-to-identity:4d6c254665 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-330fbc6f4c"></a>
- <a id="s-c75474c3be"></a>`distribution`: `stove0-protocol`
- <a id="s-7cfacab825"></a>`module`: `stove0_protocol`
- <a id="s-7ce2016142"></a>`name`: `to_identity`
- <a id="s-0956cbbf04"></a>`owner`: `stove0_protocol.OperationRef`
- <a id="s-d3da5d4835"></a>`unit`: `member`

### Declared structure

- <a id="s-61ecd94544"></a>`kind`: `"method"`
- <a id="s-bfcfaafda1"></a>`signature`: `"\"(self) -> 'OperationIdentity'\""`

## Maintained corroboration

### Related interface records

- [OperationRef](stove0-protocol-operationref.md)

## Governing policies

- <a id="pa-38c876eddf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.OperationRef.to_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f1199b8dff848951a01836ffed22e283345ef43897f98a76728a97f7b13aae8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'OperationIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "to_identity",
  "owner": "stove0_protocol.OperationRef",
  "unit": "member"
}
```
