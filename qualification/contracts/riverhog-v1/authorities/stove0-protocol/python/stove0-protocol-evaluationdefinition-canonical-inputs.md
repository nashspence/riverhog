# stove0_protocol.EvaluationDefinition.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinition-cano-785b3681fc:64c5598de9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-03cabb9982"></a>
- <a id="s-6e47c7f30f"></a>`distribution`: `stove0-protocol`
- <a id="s-080863ccc7"></a>`module`: `stove0_protocol`
- <a id="s-3be0bd6aa3"></a>`name`: `canonical_inputs`
- <a id="s-1531d9aae2"></a>`owner`: `stove0_protocol.EvaluationDefinition`
- <a id="s-0a4b014710"></a>`unit`: `member`

### Declared structure

- <a id="s-b0bb6429dc"></a>`kind`: `"classmethod"`
- <a id="s-318a39c134"></a>`signature`: `"\"(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [EvaluationDefinition](stove0-protocol-evaluationdefinition.md)

## Governing policies

- <a id="pa-ed76182de4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinition.canonical_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6914051de838fdedc6611b0b9d9459156fddd5246b627ee056012e1f6e36f4b1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_inputs",
  "owner": "stove0_protocol.EvaluationDefinition",
  "unit": "member"
}
```

</details>
