# stove0_protocol.EvaluationMatrix.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationmatrix-seal:732f035c22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7d1beb0b2"></a>
| Field | Shape |
|---|---|
| <a id="s-f44401f8d3"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-801525b73b"></a>`distribution` | "stove0-protocol" |
| <a id="s-b91bab7a6c"></a>`module` | "stove0_protocol" |
| <a id="s-6fd870ddd7"></a>`name` | "seal" |
| <a id="s-c578660a66"></a>`owner` | "stove0_protocol.EvaluationMatrix" |
| <a id="s-3c8afd9fc5"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationMatrix](stove0-protocol-evaluationmatrix.md)

## Governing policies

- <a id="pa-3935849a0b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationMatrix.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab026a856449c8be2111faf7332f3452735726616e8a3b8edc89f67835463f20 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'EvaluationMatrixPayload') -> 'EvaluationMatrix'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.EvaluationMatrix",
  "unit": "member"
}
```
