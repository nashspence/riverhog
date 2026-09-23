# a_gogurt_windows_listener.TaskSchedulerUserAdapter.unregister

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-taskschedulerus-285b7c45ee:db052365ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b54c7e100"></a>
- <a id="s-efe0e60e86"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-f367c4e2a1"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-17ba429cc5"></a>`name`: `unregister`
- <a id="s-e326d4efc6"></a>`owner`: `a_gogurt_windows_listener.TaskSchedulerUserAdapter`
- <a id="s-6ce0306764"></a>`unit`: `member`

### Declared structure

- <a id="s-735b3c2af0"></a>`kind`: `"method"`
- <a id="s-3c5618d786"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](a-gogurt-windows-listener-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-355946f96d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.TaskSchedulerUserAdapter.unregister`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e576bbd486394887cb1f51f4000130e96b7a40df23f5398bbcfb84f86cbc2936 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "unregister",
  "owner": "a_gogurt_windows_listener.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
