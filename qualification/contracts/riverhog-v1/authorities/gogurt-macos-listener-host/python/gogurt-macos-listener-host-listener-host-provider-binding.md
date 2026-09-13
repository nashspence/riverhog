# gogurt_macos_listener_host.LISTENER_HOST_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-listener-host-67f84290e7:5f49dae63b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-411dcc7b37"></a>
| Field | Shape |
|---|---|
| <a id="s-022973c4e5"></a>`contract` | type="gogurt_listener_runtime.platform.ListenerHostProviderBinding"; additional keys=`kind` |
| <a id="s-7f51a1ee5a"></a>`distribution` | "gogurt-macos-listener-host" |
| <a id="s-76f1150d9b"></a>`module` | "gogurt_macos_listener_host" |
| <a id="s-929e54ee86"></a>`name` | "LISTENER_HOST_PROVIDER_BINDING" |
| <a id="s-44f6801ae2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-fb4219aa7f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.LISTENER_HOST_PROVIDER_BINDING`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37b7ae568db17f4cf77ea255cc1b59df4e0d325c51b6daa0b54fbe0a260d87cd -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "LISTENER_HOST_PROVIDER_BINDING",
  "unit": "export"
}
```
