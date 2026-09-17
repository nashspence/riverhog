# gogurt_linux_listener_host.SystemdUserAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduseradapter:a129e46324 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-434a19fe2b"></a>
- <a id="s-4fa0c6f749"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-37aa751729"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-8921b46033"></a>`name`: `SystemdUserAdapter`
- <a id="s-eb59ef285e"></a>`unit`: `export`

### Declared structure

- <a id="s-99aa78344d"></a>`kind`: `"class"`
- <a id="s-1748aabdbb"></a>`signature`: `"\"(registration_file: 'Path') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [process_is_running](gogurt-linux-listener-host-systemduseradapter-process-is-running.md)
- [unregister](gogurt-linux-listener-host-systemduseradapter-unregister.md)
- [register](gogurt-linux-listener-host-systemduseradapter-register.md)
- [start](gogurt-linux-listener-host-systemduseradapter-start.md)
- [status](gogurt-linux-listener-host-systemduseradapter-status.md)
- [stop](gogurt-linux-listener-host-systemduseradapter-stop.md)

## Governing policies

- <a id="pa-89e103c89c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — [reference/gogurt/listener-host/linux/src/gogurt\_linux\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21a5a861a80be5411cd3d37c35f8a526d172eb9c85d95f2ac97eaeba8eb9fff5 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(registration_file: 'Path') -> 'None'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "SystemdUserAdapter",
  "unit": "export"
}
```

</details>
