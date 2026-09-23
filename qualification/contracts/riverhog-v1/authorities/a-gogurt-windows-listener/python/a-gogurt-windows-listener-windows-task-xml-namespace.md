# a_gogurt_windows_listener.WINDOWS_TASK_XML_NAMESPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-windows-task-xml-namespace:ad2b2f2d8a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b5fc27f827"></a>
- <a id="s-bda42440d4"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-09094647fe"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-ce10f48ed6"></a>`name`: `WINDOWS_TASK_XML_NAMESPACE`
- <a id="s-22ee770dda"></a>`unit`: `export`

### Declared structure

- <a id="s-524a3779af"></a>`kind`: `"constant"`
- <a id="s-c8474e90e0"></a>`value`: `"http://schemas.microsoft.com/windows/2004/02/mit/task"`

## Governing policies

- <a id="pa-c6f6f7ff96"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.WINDOWS_TASK_XML_NAMESPACE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40cb15e239d78c47f3ef7371685f04e0c925f70fb3878b699010106c76ed0183 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "http://schemas.microsoft.com/windows/2004/02/mit/task"
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "WINDOWS_TASK_XML_NAMESPACE",
  "unit": "export"
}
```

</details>
