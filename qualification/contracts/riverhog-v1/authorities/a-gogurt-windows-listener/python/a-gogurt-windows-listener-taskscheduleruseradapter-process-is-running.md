# a_gogurt_windows_listener.TaskSchedulerUserAdapter.process_is_running

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-taskschedulerus-770f4b2229:e775ceec4f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-13b6b998f4"></a>
- <a id="s-4a0397ebda"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-a209670728"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-fb6a8191e1"></a>`name`: `process_is_running`
- <a id="s-993698226b"></a>`owner`: `a_gogurt_windows_listener.TaskSchedulerUserAdapter`
- <a id="s-bed407e929"></a>`unit`: `member`

### Declared structure

- <a id="s-3f887517f8"></a>`kind`: `"staticmethod"`
- <a id="s-546c756948"></a>`signature`: `"\"(pid: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](a-gogurt-windows-listener-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-2bffaff000"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.TaskSchedulerUserAdapter.process_is_running`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02d01d5a9ed3c3b05241342492b71a4abb0f5b3c340747e7712ac88904a09ca5 -->

```json
{
  "contract": {
    "kind": "staticmethod",
    "signature": "\"(pid: 'int') -> 'bool'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "process_is_running",
  "owner": "a_gogurt_windows_listener.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
