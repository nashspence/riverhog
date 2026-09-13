# gogurt_listener_runtime.ListenerStore.runnable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore-runnable:dc26c785e9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1114a4a074"></a>
| Field | Shape |
|---|---|
| <a id="s-e0722c0de6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d7c9de9689"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-a7a1b2a7aa"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-e32c8a26ff"></a>`name` | "runnable" |
| <a id="s-cdd75bb6db"></a>`owner` | "gogurt_listener_runtime.ListenerStore" |
| <a id="s-67a242cc83"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_listener_runtime.ListenerStore](gogurt-listener-runtime-listenerstore.md)

## Governing policies

- <a id="pa-51ffb79d9d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore.runnable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ff4475bd6b5c9142f8a4b4e38d4c36ea8227926cd836d3e2e918707fc506d5b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float', limit: 'int') -> 'list[str]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "runnable",
  "owner": "gogurt_listener_runtime.ListenerStore",
  "unit": "member"
}
```
