# gogurt_listener_runtime.ListenerStore

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore:d03bc8c810 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed2f15669c"></a>
- <a id="s-079aa27486"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-28e87b3a81"></a>`module`: `gogurt_listener_runtime`
- <a id="s-118178382f"></a>`name`: `ListenerStore`
- <a id="s-8fcedc7f2e"></a>`unit`: `export`

### Declared structure

- <a id="s-7cec788338"></a>`kind`: `"class"`
- <a id="s-c05035bfc4"></a>`signature`: `"\"(path: 'Path') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [create](gogurt-listener-runtime-listenerstore-create.md)
- [finish_dispatch](gogurt-listener-runtime-listenerstore-finish-dispatch.md)
- [mark_running_uncertain](gogurt-listener-runtime-listenerstore-mark-running-uncertain.md)
- [observe](gogurt-listener-runtime-listenerstore-observe.md)
- [runnable](gogurt-listener-runtime-listenerstore-runnable.md)
- [start_dispatch](gogurt-listener-runtime-listenerstore-start-dispatch.md)
- [summary](gogurt-listener-runtime-listenerstore-summary.md)

## Governing policies

- <a id="pa-24f85d2878"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7faa1ead53f5b079c8f5f5157aa1d99a0f7858f8c309381afd7eafff643574ae -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(path: 'Path') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ListenerStore",
  "unit": "export"
}
```

</details>
