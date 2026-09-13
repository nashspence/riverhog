# riverhog_protocol.validate_lifecycle_event_cursor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-validate-lifecycle-event-cursor:4070cada15 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b73d3359f"></a>
| Field | Shape |
|---|---|
| <a id="s-74fb8db3d2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f5622b2999"></a>`distribution` | "riverhog-protocol" |
| <a id="s-4784e365ab"></a>`module` | "riverhog_protocol" |
| <a id="s-c987e9e0a2"></a>`name` | "validate_lifecycle_event_cursor" |
| <a id="s-54d9a85c00"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a91074daed"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.validate_lifecycle_event_cursor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7398c586082c606a04b4351f080c5e3bd974bcf822b655cc673145b08a63e13 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_lifecycle_event_cursor",
  "unit": "export"
}
```
