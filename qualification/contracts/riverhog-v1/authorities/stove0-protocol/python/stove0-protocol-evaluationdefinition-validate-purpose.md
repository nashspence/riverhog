# stove0_protocol.EvaluationDefinition.validate_purpose

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinition-vali-9b552d1c7b:ceff6c3ba2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-38bedc1bfd"></a>
- <a id="s-f950dec6da"></a>`distribution`: `stove0-protocol`
- <a id="s-e8b461a0e6"></a>`module`: `stove0_protocol`
- <a id="s-2ddbe71093"></a>`name`: `validate_purpose`
- <a id="s-3d7cdbdcde"></a>`owner`: `stove0_protocol.EvaluationDefinition`
- <a id="s-2e1bc1ec2d"></a>`unit`: `member`

### Declared structure

- <a id="s-9b0fdd5464"></a>`kind`: `"method"`
- <a id="s-48dd816e79"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [EvaluationDefinition](stove0-protocol-evaluationdefinition.md)

## Governing policies

- <a id="pa-1351c99ae5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinition.validate_purpose`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca85eaa3fd4d54ed672a4c8f9740274e68692dee490a32d4aec316bac26b0a16 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "validate_purpose",
  "owner": "stove0_protocol.EvaluationDefinition",
  "unit": "member"
}
```

</details>
