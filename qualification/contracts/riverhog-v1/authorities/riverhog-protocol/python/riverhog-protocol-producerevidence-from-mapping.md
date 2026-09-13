# riverhog_protocol.ProducerEvidence.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-producerevidence-from-mapping:d07cc8aab9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df807d96be"></a>
| Field | Shape |
|---|---|
| <a id="s-fe310bf94d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-38d39dc385"></a>`distribution` | "riverhog-protocol" |
| <a id="s-9217945c53"></a>`module` | "riverhog_protocol" |
| <a id="s-1ebe74839b"></a>`name` | "from_mapping" |
| <a id="s-c955a8314c"></a>`owner` | "riverhog_protocol.ProducerEvidence" |
| <a id="s-7b99bd6c78"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProducerEvidence](riverhog-protocol-producerevidence.md)

## Governing policies

- <a id="pa-9d4b56e376"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProducerEvidence.from_mapping`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d31cc002aa8f9252c21d30cb3c81fc337f7bd0d141660da2be13ebf85f81bb5 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'ProducerEvidence'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.ProducerEvidence",
  "unit": "member"
}
```
