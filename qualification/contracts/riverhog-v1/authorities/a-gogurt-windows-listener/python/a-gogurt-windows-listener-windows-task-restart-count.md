# a_gogurt_windows_listener.WINDOWS_TASK_RESTART_COUNT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-windows-task-restart-count:fd4b4b1303 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6add724a46"></a>
- <a id="s-3f6a1e18bf"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-0fa8d32d5a"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-143991bdde"></a>`name`: `WINDOWS_TASK_RESTART_COUNT`
- <a id="s-120e135310"></a>`unit`: `export`

### Declared structure

- <a id="s-e391caa946"></a>`kind`: `"constant"`
- <a id="s-b4b6ad31b2"></a>`value`: `3`

## Governing policies

- <a id="pa-e96701cbf8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.WINDOWS_TASK_RESTART_COUNT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 349a2e0b078aef54d69e0c7c84e9d547d83ad7258398d24f2f77f92b807675e7 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 3
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "WINDOWS_TASK_RESTART_COUNT",
  "unit": "export"
}
```

</details>
