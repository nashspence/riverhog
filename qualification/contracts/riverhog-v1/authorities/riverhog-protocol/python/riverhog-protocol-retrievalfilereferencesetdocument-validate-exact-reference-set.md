# riverhog_protocol.RetrievalFileReferenceSetDocument.validate_exact_reference_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalfilereferences-7da2b3cd0c:6fd3ec7805 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a7350ba79f"></a>
| Field | Shape |
|---|---|
| <a id="s-2dcb8b7101"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b5e11e673c"></a>`distribution` | "riverhog-protocol" |
| <a id="s-22b57529f2"></a>`module` | "riverhog_protocol" |
| <a id="s-6046dc98e1"></a>`name` | "validate_exact_reference_set" |
| <a id="s-a6349e3eee"></a>`owner` | "riverhog_protocol.RetrievalFileReferenceSetDocument" |
| <a id="s-7536085f91"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.RetrievalFileReferenceSetDocument](riverhog-protocol-retrievalfilereferencesetdocument.md)

## Governing policies

- <a id="pa-dc2d3b6258"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalFileReferenceSetDocument.validate_exact_reference_set`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9cd3fb62d44180bcbc69e3477a0b915995def3c993116b407da775968369176 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_exact_reference_set",
  "owner": "riverhog_protocol.RetrievalFileReferenceSetDocument",
  "unit": "member"
}
```
