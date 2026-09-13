# gogurt_windows_listener_host.listener_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-listener-adapter:fab71002fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a0fc05a568"></a>
| Field | Shape |
|---|---|
| <a id="s-dca234fcc9"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-de899bc05e"></a>`distribution` | "gogurt-windows-listener-host" |
| <a id="s-8c3d1f711d"></a>`module` | "gogurt_windows_listener_host" |
| <a id="s-845c0ffa02"></a>`name` | "listener_adapter" |
| <a id="s-9bdc0046b7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-93db8c3af4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.listener_adapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55128621ec23fc7ce72afdeefcf8e65b5daee83a2070d2f7fa1dd2aea7422547 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'ListenerAdapter'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "listener_adapter",
  "unit": "export"
}
```
