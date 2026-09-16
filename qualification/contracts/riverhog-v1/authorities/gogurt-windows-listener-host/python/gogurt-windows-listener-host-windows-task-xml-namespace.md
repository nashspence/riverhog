# gogurt_windows_listener_host.WINDOWS_TASK_XML_NAMESPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-windows-task-4dcde8c413:22670e4b09 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ffe2c808b"></a>
- <a id="s-fb88778155"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-8b38b02873"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-586f9edf8b"></a>`name`: `WINDOWS_TASK_XML_NAMESPACE`
- <a id="s-1838bad94f"></a>`unit`: `export`

### Declared structure

- <a id="s-ab04410000"></a>`kind`: `"constant"`
- <a id="s-c576952227"></a>`value`: `"http://schemas.microsoft.com/windows/2004/02/mit/task"`

## Governing policies

- <a id="pa-367270521e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.WINDOWS_TASK_XML_NAMESPACE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99ee5ddb65f5e2e4ebd7af7724fc3ddb2cbbc6f4e1ba2188e11e915f73a34bca -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "http://schemas.microsoft.com/windows/2004/02/mit/task"
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "WINDOWS_TASK_XML_NAMESPACE",
  "unit": "export"
}
```

</details>
