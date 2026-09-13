# gogurt_windows_listener_host.LISTENER_HOST_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-listener-hos-3380202d9d:946f3d1424 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8120710623"></a>
| Field | Shape |
|---|---|
| <a id="s-3058656ecd"></a>`contract` | type="gogurt_listener_runtime.platform.ListenerHostProviderBinding"; additional keys=`kind` |
| <a id="s-2347d8a051"></a>`distribution` | "gogurt-windows-listener-host" |
| <a id="s-8bb4244f52"></a>`module` | "gogurt_windows_listener_host" |
| <a id="s-c2fab9c0de"></a>`name` | "LISTENER_HOST_PROVIDER_BINDING" |
| <a id="s-0af5c78430"></a>`unit` | "export" |

## Governing policies

- <a id="pa-7092ad8877"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.LISTENER_HOST_PROVIDER_BINDING`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc2d28b2797dbf7507df1ff6e812d691f699a7c50a4b9ffdd1be4ca647648032 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "LISTENER_HOST_PROVIDER_BINDING",
  "unit": "export"
}
```
