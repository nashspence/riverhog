# a_gogurt_windows_listener.windows_task_name

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-windows-task-name:aa9036d3d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-080c1d1582"></a>
- <a id="s-7e52bd67b1"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-e4720f7bbf"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-9839d07a7c"></a>`name`: `windows_task_name`
- <a id="s-6ebf5c768e"></a>`unit`: `export`

### Declared structure

- <a id="s-295a91b591"></a>`kind`: `"function"`
- <a id="s-65e194d570"></a>`signature`: `"\"(user_sid: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-73c29ebe55"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.windows_task_name`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42ae0d55f6c00d743b2a93297a4020f62b45c5c3b080120ab5e4b34320a7ef02 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(user_sid: 'str') -> 'str'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "windows_task_name",
  "unit": "export"
}
```

</details>
