# gogurt_listener_runtime.ListenerRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerruntime:b148609f27 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bd44e49725"></a>
- <a id="s-a28a3e041d"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-3da0597115"></a>`module`: `gogurt_listener_runtime`
- <a id="s-d5046414da"></a>`name`: `ListenerRuntime`
- <a id="s-fcbd115fbe"></a>`unit`: `export`

### Declared structure

- <a id="s-12b1ae6980"></a>`kind`: `"class"`
- <a id="s-7e9acd5629"></a>`signature`: `"\"(config: 'ListenerConfig', paths: 'ListenerRuntimePaths', *, mounted_volume_provider: 'MountedVolumeProvider', product_version: 'str', clock: 'Callable[[], float]' = <built-in function time>, sleep: 'Callable[[float], None]' = <built-in function sleep>, logger: 'logging.Logger \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [request_stop](gogurt-listener-runtime-listenerruntime-request-stop.md)
- [run_once](gogurt-listener-runtime-listenerruntime-run-once.md)
- [run](gogurt-listener-runtime-listenerruntime-run.md)

## Governing policies

- <a id="pa-c66a14c88f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerRuntime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a64ded6bd1030bbf1f54aad7f26a52ff2b9b7fc8d4ffa1de6581ab248a3bd8a1 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(config: 'ListenerConfig', paths: 'ListenerRuntimePaths', *, mounted_volume_provider: 'MountedVolumeProvider', product_version: 'str', clock: 'Callable[[], float]' = <built-in function time>, sleep: 'Callable[[float], None]' = <built-in function sleep>, logger: 'logging.Logger | None' = None) -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ListenerRuntime",
  "unit": "export"
}
```

</details>
