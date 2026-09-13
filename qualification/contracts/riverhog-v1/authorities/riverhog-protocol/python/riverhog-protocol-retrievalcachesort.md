# riverhog_protocol.RetrievalCacheSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalcachesort:419706288f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4f77c3a3d2"></a>
| Field | Shape |
|---|---|
| <a id="s-9f3c402fa8"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-b29cfbb37c"></a>`distribution` | "riverhog-protocol" |
| <a id="s-26e0864071"></a>`module` | "riverhog_protocol" |
| <a id="s-b97574ae56"></a>`name` | "RetrievalCacheSort" |
| <a id="s-1dcc31db73"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0c7aeb46a5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalCacheSort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 252c8d7331756a83b2e43e722afca32ae5816c83b1f12bba1b96e4d1463fca66 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['collection_id', 'source_store', 'object_id', 'stored_bytes', 'cached_at', 'verified_at', 'protected_until']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalCacheSort",
  "unit": "export"
}
```
