# riverhog_protocol.RetrievalCacheState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalcachestate:73df6c6e4c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-862af1de4c"></a>
| Field | Shape |
|---|---|
| <a id="s-ea6e568c56"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-dd5ff7ad41"></a>`distribution` | "riverhog-protocol" |
| <a id="s-f1c81e781a"></a>`module` | "riverhog_protocol" |
| <a id="s-86aa820d37"></a>`name` | "RetrievalCacheState" |
| <a id="s-05bb66c607"></a>`unit` | "export" |

## Governing policies

- <a id="pa-070f751802"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalCacheState`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98bef52c755693ceb921c167bb135537b6dea02d636151d82b41df5a81de46bd -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['ready', 'delete_pending', 'deleting']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalCacheState",
  "unit": "export"
}
```
