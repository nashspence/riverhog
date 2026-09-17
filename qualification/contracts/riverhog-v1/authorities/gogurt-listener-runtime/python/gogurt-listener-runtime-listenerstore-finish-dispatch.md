# gogurt_listener_runtime.ListenerStore.finish_dispatch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore-fin-a3ce799553:afe10679c8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c3cbfc7c5e"></a>
- <a id="s-7f95405715"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-f21a6a60eb"></a>`module`: `gogurt_listener_runtime`
- <a id="s-75fbdf2114"></a>`name`: `finish_dispatch`
- <a id="s-a0d1f5930a"></a>`owner`: `gogurt_listener_runtime.ListenerStore`
- <a id="s-46055ae503"></a>`unit`: `member`

### Declared structure

- <a id="s-93cc797ddd"></a>`kind`: `"method"`
- <a id="s-1244d7e0d5"></a>`signature`: `"\"(self, dispatch_id: 'str', *, return_code: 'int \| None', error: 'str \| None', uncertain: 'bool' = False, now: 'float') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ListenerStore](gogurt-listener-runtime-listenerstore.md)

## Governing policies

- <a id="pa-e20963153a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore.finish_dispatch`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd9235ba3554084b46a51835eac0d601fdc54d930bb474217d28175902ab40cc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, dispatch_id: 'str', *, return_code: 'int | None', error: 'str | None', uncertain: 'bool' = False, now: 'float') -> 'str'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "finish_dispatch",
  "owner": "gogurt_listener_runtime.ListenerStore",
  "unit": "member"
}
```

</details>
