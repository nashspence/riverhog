# stove0_protocol.EvaluationMatrix.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationmatrix-verify-digest:d0e6fb3b0d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-950c966c9f"></a>
| Field | Shape |
|---|---|
| <a id="s-e69a840e62"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-376e166288"></a>`distribution` | "stove0-protocol" |
| <a id="s-70920b8eef"></a>`module` | "stove0_protocol" |
| <a id="s-0ae587ed22"></a>`name` | "verify_digest" |
| <a id="s-1753ccaf54"></a>`owner` | "stove0_protocol.EvaluationMatrix" |
| <a id="s-a324e87f1d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationMatrix](stove0-protocol-evaluationmatrix.md)

## Governing policies

- <a id="pa-9f94588848"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationMatrix.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 210f6dd6da708167d719020cfe5cdb85e1db5d8b91ab2cf7dd3768c25793f9f3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.EvaluationMatrix",
  "unit": "member"
}
```
