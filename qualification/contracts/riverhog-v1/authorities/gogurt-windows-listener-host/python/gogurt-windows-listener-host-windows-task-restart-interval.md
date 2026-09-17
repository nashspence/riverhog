# gogurt_windows_listener_host.WINDOWS_TASK_RESTART_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-windows-task-fc4a2196d4:6fc632901a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-13a01db581"></a>
- <a id="s-e3a9cb5230"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-d594148a74"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-d278bf304b"></a>`name`: `WINDOWS_TASK_RESTART_INTERVAL`
- <a id="s-f13e587c85"></a>`unit`: `export`

### Declared structure

- <a id="s-04f16c6091"></a>`kind`: `"constant"`
- <a id="s-123be213ab"></a>`value`: `"PT1M"`

## Governing policies

- <a id="pa-0e5c25d0fe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.WINDOWS_TASK_RESTART_INTERVAL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 36a6297e71901f595a18fc6121193a977738d3f4cbb4ea15dd23a36360bf43d9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "PT1M"
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "WINDOWS_TASK_RESTART_INTERVAL",
  "unit": "export"
}
```

</details>
