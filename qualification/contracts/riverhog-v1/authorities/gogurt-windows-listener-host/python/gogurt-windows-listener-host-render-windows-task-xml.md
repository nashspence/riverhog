# gogurt_windows_listener_host.render_windows_task_xml

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-render-windows-task-xml:dd74003dbc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5679f056b2"></a>
- <a id="s-98520292d0"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-4432a69231"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-342eef2f71"></a>`name`: `render_windows_task_xml`
- <a id="s-f685641724"></a>`unit`: `export`

### Declared structure

- <a id="s-dfffe46967"></a>`kind`: `"function"`
- <a id="s-db83967d94"></a>`signature`: `"\"(command: 'Sequence[str]', *, user_sid: 'str') -> 'bytes'\""`

## Governing policies

- <a id="pa-44d3c4ed7d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.render_windows_task_xml`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 70413b61f796dc126daf7b627cc07511c017451a971388d9b3c4e26511cffac8 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(command: 'Sequence[str]', *, user_sid: 'str') -> 'bytes'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "render_windows_task_xml",
  "unit": "export"
}
```

</details>
