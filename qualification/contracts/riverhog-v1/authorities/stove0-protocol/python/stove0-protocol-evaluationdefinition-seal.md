# stove0_protocol.EvaluationDefinition.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinition-seal:779a5f16ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-280ae6726a"></a>
- <a id="s-7502b062c9"></a>`distribution`: `stove0-protocol`
- <a id="s-337ca190fc"></a>`module`: `stove0_protocol`
- <a id="s-dc5f65dffc"></a>`name`: `seal`
- <a id="s-3f99bce40c"></a>`owner`: `stove0_protocol.EvaluationDefinition`
- <a id="s-47fb098784"></a>`unit`: `member`

### Declared structure

- <a id="s-799caae0f1"></a>`kind`: `"classmethod"`
- <a id="s-097f9cdcff"></a>`signature`: `"\"(cls, payload: 'EvaluationDefinitionPayload') -> 'EvaluationDefinition'\""`

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationDefinition](stove0-protocol-evaluationdefinition.md)

## Governing policies

- <a id="pa-287e62b97d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinition.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a9a5383bdebc47a456aae88d01f3912e1258b3cc8e67bc309331149847b8f15 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'EvaluationDefinitionPayload') -> 'EvaluationDefinition'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.EvaluationDefinition",
  "unit": "member"
}
```
