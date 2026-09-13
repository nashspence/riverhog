# riverhog_protocol.MemoryCollectionTagNodeStore.put

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-memorycollectiontagnodestore-put:2a6f52fa9e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23149ea22f"></a>
| Field | Shape |
|---|---|
| <a id="s-a46e2fcc8f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1c4e41f909"></a>`distribution` | "riverhog-protocol" |
| <a id="s-0c9bd4a592"></a>`module` | "riverhog_protocol" |
| <a id="s-e99ad658bb"></a>`name` | "put" |
| <a id="s-c07713fff6"></a>`owner` | "riverhog_protocol.MemoryCollectionTagNodeStore" |
| <a id="s-38f1f9da8a"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.MemoryCollectionTagNodeStore](riverhog-protocol-memorycollectiontagnodestore.md)

## Governing policies

- <a id="pa-3f3289f730"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.MemoryCollectionTagNodeStore.put`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c795492126be83598aaef7ac675cc1544e173d1ed914d641cb5b5afe40f3b05f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, digest: 'str', encoded: 'bytes') -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "put",
  "owner": "riverhog_protocol.MemoryCollectionTagNodeStore",
  "unit": "member"
}
```
