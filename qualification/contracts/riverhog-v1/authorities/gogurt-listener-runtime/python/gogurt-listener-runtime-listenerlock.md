# gogurt_listener_runtime.ListenerLock

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerlock:b7f7573941 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-677386d14b"></a>
- <a id="s-d185f8e647"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-9df5bf7bc7"></a>`module`: `gogurt_listener_runtime`
- <a id="s-9b96006a3d"></a>`name`: `ListenerLock`
- <a id="s-3766e53090"></a>`unit`: `export`

### Declared structure

- <a id="s-aae8c667b4"></a>`kind`: `"class"`
- <a id="s-30d5522e3d"></a>`signature`: `"\"(path: 'Path') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [__enter__](gogurt-listener-runtime-listenerlock-enter.md)
- [__exit__](gogurt-listener-runtime-listenerlock-exit.md)

## Governing policies

- <a id="pa-5b228b5aee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerLock`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76fd0e0a81422759995e6779adbda0408261bbe6628751e37cadba68a843d706 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(path: 'Path') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ListenerLock",
  "unit": "export"
}
```

</details>
