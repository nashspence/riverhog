# a_gogurt_windows_listener.TaskSchedulerUserAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-taskscheduleruseradapter:fed27d2ac7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-604f6625ec"></a>
- <a id="s-14d0ba31e3"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-5b77ce1348"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-3454ac234c"></a>`name`: `TaskSchedulerUserAdapter`
- <a id="s-4da4002f8c"></a>`unit`: `export`

### Declared structure

- <a id="s-78355bbdd8"></a>`kind`: `"class"`
- <a id="s-b0c94b4707"></a>`signature`: `"'()'"`

## Maintained corroboration

### Related interface records

- [unregister](a-gogurt-windows-listener-taskscheduleruseradapter-unregister.md)
- [register](a-gogurt-windows-listener-taskscheduleruseradapter-register.md)
- [start](a-gogurt-windows-listener-taskscheduleruseradapter-start.md)
- [process_is_running](a-gogurt-windows-listener-taskscheduleruseradapter-process-is-running.md)
- [status](a-gogurt-windows-listener-taskscheduleruseradapter-status.md)
- [stop](a-gogurt-windows-listener-taskscheduleruseradapter-stop.md)

## Governing policies

- <a id="pa-3502c67478"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.TaskSchedulerUserAdapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5cd0c33c5e73558b67bc837bc210f6d8dadf8c4363dffdd002fa7fd2fe451679 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'()'"
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "TaskSchedulerUserAdapter",
  "unit": "export"
}
```

</details>
