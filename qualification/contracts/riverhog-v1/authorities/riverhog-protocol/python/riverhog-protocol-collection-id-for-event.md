# riverhog_protocol.collection_id_for_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-id-for-event:6c975c1332 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cb014d3fb8"></a>
| Field | Shape |
|---|---|
| <a id="s-c4a1e8061e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-984990ea21"></a>`distribution` | "riverhog-protocol" |
| <a id="s-4b6c3bb021"></a>`module` | "riverhog_protocol" |
| <a id="s-1d72cf6d9c"></a>`name` | "collection_id_for_event" |
| <a id="s-c122b298df"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a18c60a670"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_id_for_event`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5c84f6d712cad1a1218f3f7a1776953b900ba9b9a5e63ff6bc18be905c4f50c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'CloudEvent | dict[str, Any]') -> 'int'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_id_for_event",
  "unit": "export"
}
```
