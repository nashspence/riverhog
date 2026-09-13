# gogurt_linux_listener_host.default_listener_paths

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-default-listener-paths:487214e708 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b35b9823c"></a>
| Field | Shape |
|---|---|
| <a id="s-519a891b58"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6cece46b49"></a>`distribution` | "gogurt-linux-listener-host" |
| <a id="s-dd965e6726"></a>`module` | "gogurt_linux_listener_host" |
| <a id="s-4939359cf4"></a>`name` | "default_listener_paths" |
| <a id="s-c4943cb217"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a0da66c057"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.default_listener_paths`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c99e36326c9adebe9a4bbf756889b5ccde988ef4857eaf581581b7d5a710bfed -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerRuntimePaths'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "default_listener_paths",
  "unit": "export"
}
```
