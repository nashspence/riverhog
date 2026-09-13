# gogurt_macos_listener_host.LaunchdUserAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-launchduseradapter:cb4a646b3e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42286dc77b"></a>
| Field | Shape |
|---|---|
| <a id="s-8678b66d9b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e42a527296"></a>`distribution` | "gogurt-macos-listener-host" |
| <a id="s-fc7b5dce4f"></a>`module` | "gogurt_macos_listener_host" |
| <a id="s-6ecb71cc55"></a>`name` | "LaunchdUserAdapter" |
| <a id="s-29f0d2ec0c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [gogurt_macos_listener_host.LaunchdUserAdapter.register](gogurt-macos-listener-host-launchduseradapter-register.md)
- [gogurt_macos_listener_host.LaunchdUserAdapter.process_is_running](gogurt-macos-listener-host-launchduseradapter-process-is-running.md)
- [gogurt_macos_listener_host.LaunchdUserAdapter.unregister](gogurt-macos-listener-host-launchduseradapter-unregister.md)
- [gogurt_macos_listener_host.LaunchdUserAdapter.start](gogurt-macos-listener-host-launchduseradapter-start.md)
- [gogurt_macos_listener_host.LaunchdUserAdapter.status](gogurt-macos-listener-host-launchduseradapter-status.md)
- [gogurt_macos_listener_host.LaunchdUserAdapter.stop](gogurt-macos-listener-host-launchduseradapter-stop.md)

## Governing policies

- <a id="pa-86974cb637"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.LaunchdUserAdapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e89a0ce39c1db211ad577e2791582eca9394aa9cc8977171470e8b4c252acda0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(registration_file: 'Path') -> 'None'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "LaunchdUserAdapter",
  "unit": "export"
}
```
