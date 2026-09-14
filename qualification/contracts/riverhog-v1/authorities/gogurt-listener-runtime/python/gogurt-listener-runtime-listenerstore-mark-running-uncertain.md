# gogurt_listener_runtime.ListenerStore.mark_running_uncertain

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore-mar-6d128d964a:90e1e2cf0e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7cbcf98e17"></a>
- <a id="s-f88a1be4c9"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-38cbda9448"></a>`module`: `gogurt_listener_runtime`
- <a id="s-14465a1715"></a>`name`: `mark_running_uncertain`
- <a id="s-284c036e51"></a>`owner`: `gogurt_listener_runtime.ListenerStore`
- <a id="s-6db91584dd"></a>`unit`: `member`

### Declared structure

- <a id="s-4c8a669c3b"></a>`kind`: `"method"`
- <a id="s-913ad5be2d"></a>`signature`: `"\"(self, dispatch_id: 'str', *, error: 'str', now: 'float') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ListenerStore](gogurt-listener-runtime-listenerstore.md)

## Governing policies

- <a id="pa-ed98ae6ee9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore.mark_running_uncertain`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 908dbf72f763ecf7173cf5f720d98b978a46c332f6d681fe942ee8cb0877ed74 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, dispatch_id: 'str', *, error: 'str', now: 'float') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "mark_running_uncertain",
  "owner": "gogurt_listener_runtime.ListenerStore",
  "unit": "member"
}
```
