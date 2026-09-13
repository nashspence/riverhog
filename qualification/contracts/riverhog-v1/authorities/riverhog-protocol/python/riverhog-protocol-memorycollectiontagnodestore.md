# riverhog_protocol.MemoryCollectionTagNodeStore

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-memorycollectiontagnodestore:ecf3f83f5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-49b5b5ef57"></a>
| Field | Shape |
|---|---|
| <a id="s-b519a07a12"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c1436bbfcd"></a>`distribution` | "riverhog-protocol" |
| <a id="s-7292ceb73b"></a>`module` | "riverhog_protocol" |
| <a id="s-e285d07510"></a>`name` | "MemoryCollectionTagNodeStore" |
| <a id="s-e75237461c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.MemoryCollectionTagNodeStore.get](riverhog-protocol-memorycollectiontagnodestore-get.md)
- [riverhog_protocol.MemoryCollectionTagNodeStore.put](riverhog-protocol-memorycollectiontagnodestore-put.md)

## Governing policies

- <a id="pa-e9a97ed4df"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.MemoryCollectionTagNodeStore`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3b588d77bcd5734032bf2355bba2bda087b09fb45120797f7a52e43d2323b3f -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"() -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "MemoryCollectionTagNodeStore",
  "unit": "export"
}
```
