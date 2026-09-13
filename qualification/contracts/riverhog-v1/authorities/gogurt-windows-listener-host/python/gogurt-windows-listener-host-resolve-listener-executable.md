# gogurt_windows_listener_host.resolve_listener_executable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-resolve-list-00d7cc5b7c:50d5cbe221 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-410bed4b9e"></a>
| Field | Shape |
|---|---|
| <a id="s-1f8a870ad2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-34ae304d8e"></a>`distribution` | "gogurt-windows-listener-host" |
| <a id="s-89b7fd5882"></a>`module` | "gogurt_windows_listener_host" |
| <a id="s-21cd9d4bb2"></a>`name` | "resolve_listener_executable" |
| <a id="s-a38b8a1e1e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2a2b06d75f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.resolve_listener_executable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97d8ab9df275c33eda69ec49b915f1da46389cddda6ee62fae86ade768a15f71 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "resolve_listener_executable",
  "unit": "export"
}
```
