# a_gogurt_windows_listener.TaskSchedulerUserAdapter.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-taskschedulerus-e13cbdf42e:218d765376 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b3cf119bf8"></a>
- <a id="s-87411cd0ab"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-3083dfab6b"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-ee0cbf4a83"></a>`name`: `status`
- <a id="s-d8f4112dff"></a>`owner`: `a_gogurt_windows_listener.TaskSchedulerUserAdapter`
- <a id="s-e6a674660d"></a>`unit`: `member`

### Declared structure

- <a id="s-af8318366b"></a>`kind`: `"method"`
- <a id="s-bae774d4d1"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](a-gogurt-windows-listener-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-6659db0cb2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.TaskSchedulerUserAdapter.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9bb2a50b8295e919b352acb280ba70a7e45de6d2d6b07df368ad5311501f22e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "status",
  "owner": "a_gogurt_windows_listener.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
