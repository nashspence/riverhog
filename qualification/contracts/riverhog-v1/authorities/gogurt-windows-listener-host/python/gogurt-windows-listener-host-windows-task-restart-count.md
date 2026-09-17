# gogurt_windows_listener_host.WINDOWS_TASK_RESTART_COUNT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-windows-task-018467cdd5:7972017743 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fcb71202fc"></a>
- <a id="s-04c7d536da"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-aa2256df00"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-a4304550af"></a>`name`: `WINDOWS_TASK_RESTART_COUNT`
- <a id="s-cd2f0a5524"></a>`unit`: `export`

### Declared structure

- <a id="s-4b6d3012cd"></a>`kind`: `"constant"`
- <a id="s-ead153a8a7"></a>`value`: `3`

## Governing policies

- <a id="pa-498b1e2962"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources/authorities.md#src-ec25d3db2b) — [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.WINDOWS_TASK_RESTART_COUNT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 168eee5f7b8f204214afa6ad78bcbf892cd88c314b2a6c86e7d681f3f8b37706 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 3
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "WINDOWS_TASK_RESTART_COUNT",
  "unit": "export"
}
```

</details>
