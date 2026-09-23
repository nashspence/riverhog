# a_gogurt_windows_listener.render_windows_task_xml

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-render-windows-task-xml:52b10169e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-442c5eef04"></a>
- <a id="s-004be1a95b"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-ad6b976024"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-b553995fb1"></a>`name`: `render_windows_task_xml`
- <a id="s-f22aa3ec66"></a>`unit`: `export`

### Declared structure

- <a id="s-2ecdb37ed5"></a>`kind`: `"function"`
- <a id="s-7b265811ef"></a>`signature`: `"\"(command: 'Sequence[str]', *, user_sid: 'str') -> 'bytes'\""`

## Governing policies

- <a id="pa-89e3274363"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.render_windows_task_xml`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f7d4fe46e24bbf756e8bfe2727f4a6d453fbe0075870595af59962eae97f76e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(command: 'Sequence[str]', *, user_sid: 'str') -> 'bytes'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "render_windows_task_xml",
  "unit": "export"
}
```

</details>
