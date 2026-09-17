# gogurt_listener_runtime.ListenerStore.start_dispatch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore-start-dispatch:ed47e67c31 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bd0839019d"></a>
- <a id="s-d4b5e7ff63"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-d3dda617da"></a>`module`: `gogurt_listener_runtime`
- <a id="s-904363caf7"></a>`name`: `start_dispatch`
- <a id="s-6d1e9ab40d"></a>`owner`: `gogurt_listener_runtime.ListenerStore`
- <a id="s-7e3744ec6a"></a>`unit`: `member`

### Declared structure

- <a id="s-ceb2847f9b"></a>`kind`: `"method"`
- <a id="s-fd467ca77b"></a>`signature`: `"\"(self, dispatch_id: 'str', *, now: 'float') -> 'dict[str, object] \| None'\""`

## Maintained corroboration

### Related interface records

- [ListenerStore](gogurt-listener-runtime-listenerstore.md)

## Governing policies

- <a id="pa-319194e059"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore.start_dispatch`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc04db91db66b40d3a2bc8ac1d20a472befeb81ec7c9377fe8bebb73575c6c5c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, dispatch_id: 'str', *, now: 'float') -> 'dict[str, object] | None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "start_dispatch",
  "owner": "gogurt_listener_runtime.ListenerStore",
  "unit": "member"
}
```

</details>
