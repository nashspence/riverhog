# stove0_protocol.EvaluationMatrixPayload.canonical_variants

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationmatrixpayload-c-ba28b1d7f3:66b711b705 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b01c2c179"></a>
- <a id="s-f75529596e"></a>`distribution`: `stove0-protocol`
- <a id="s-0bc56c5cf2"></a>`module`: `stove0_protocol`
- <a id="s-e2536a2a7e"></a>`name`: `canonical_variants`
- <a id="s-f6efb0ae84"></a>`owner`: `stove0_protocol.EvaluationMatrixPayload`
- <a id="s-b444d097c9"></a>`unit`: `member`

### Declared structure

- <a id="s-77071d7fd3"></a>`kind`: `"classmethod"`
- <a id="s-d452791ab2"></a>`signature`: `"\"(cls, value: 'tuple[EvaluationVariant, ...]') -> 'tuple[EvaluationVariant, ...]'\""`

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationMatrixPayload](stove0-protocol-evaluationmatrixpayload.md)

## Governing policies

- <a id="pa-260411e637"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationMatrixPayload.canonical_variants`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a77023a7fd74fbdbaea0bdc826d9de48cd58d23d27139baff8f8cd699a0f144 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[EvaluationVariant, ...]') -> 'tuple[EvaluationVariant, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_variants",
  "owner": "stove0_protocol.EvaluationMatrixPayload",
  "unit": "member"
}
```
