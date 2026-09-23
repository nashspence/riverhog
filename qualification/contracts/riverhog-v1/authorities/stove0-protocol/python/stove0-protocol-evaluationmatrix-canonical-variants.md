# stove0_protocol.EvaluationMatrix.canonical_variants

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationmatrix-canonical-variants:036add5db3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f018225032"></a>
- <a id="s-1e773cade6"></a>`distribution`: `stove0-protocol`
- <a id="s-b851710ed2"></a>`module`: `stove0_protocol`
- <a id="s-69c3f79190"></a>`name`: `canonical_variants`
- <a id="s-4f28c25e3c"></a>`owner`: `stove0_protocol.EvaluationMatrix`
- <a id="s-1462a2d4b1"></a>`unit`: `member`

### Declared structure

- <a id="s-7125799a8f"></a>`kind`: `"classmethod"`
- <a id="s-7f2a3452b3"></a>`signature`: `"\"(cls, value: 'tuple[EvaluationVariant, ...]') -> 'tuple[EvaluationVariant, ...]'\""`

## Maintained corroboration

### Related interface records

- [EvaluationMatrix](stove0-protocol-evaluationmatrix.md)

## Governing policies

- <a id="pa-ed7a20b5fc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationMatrix.canonical_variants`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75240ee2b72c6403530e0f51cee418cadeb3302007bda9d25826eeb618e646fc -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[EvaluationVariant, ...]') -> 'tuple[EvaluationVariant, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_variants",
  "owner": "stove0_protocol.EvaluationMatrix",
  "unit": "member"
}
```

</details>
