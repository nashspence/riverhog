# stove0_protocol.EvaluationDefinition.child_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinition-child-work:f011cf8d67 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee2057faa8"></a>
- <a id="s-4ce14e126b"></a>`distribution`: `stove0-protocol`
- <a id="s-bfe3f7dae7"></a>`module`: `stove0_protocol`
- <a id="s-ff841a68c3"></a>`name`: `child_work`
- <a id="s-1efa27d89a"></a>`owner`: `stove0_protocol.EvaluationDefinition`
- <a id="s-e6e82c800f"></a>`unit`: `member`

### Declared structure

- <a id="s-cd623cd6ca"></a>`kind`: `"method"`
- <a id="s-96f80f7224"></a>`signature`: `"\"(self, variant_id: 'str') -> 'WorkIdentity'\""`

## Maintained corroboration

### Related interface records

- [EvaluationDefinition](stove0-protocol-evaluationdefinition.md)

## Governing policies

- <a id="pa-a823155593"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinition.child_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc9606b1105c56072ed3ba4c29873903c7d6646039207afe2fd52b84a36dde25 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, variant_id: 'str') -> 'WorkIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "child_work",
  "owner": "stove0_protocol.EvaluationDefinition",
  "unit": "member"
}
```
