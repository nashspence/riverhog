# gogurt_linux_listener_host.SystemdUserAdapter.process_is_running

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduserada-1c79b16af5:8367148163 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98d9e5c8e2"></a>
| Field | Shape |
|---|---|
| <a id="s-93e50d5e5e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c4dc4e1b66"></a>`distribution` | "gogurt-linux-listener-host" |
| <a id="s-b9751d1bf5"></a>`module` | "gogurt_linux_listener_host" |
| <a id="s-fe46dd6bc2"></a>`name` | "process_is_running" |
| <a id="s-e35f3a8b56"></a>`owner` | "gogurt_linux_listener_host.SystemdUserAdapter" |
| <a id="s-0033c16a76"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_linux_listener_host.SystemdUserAdapter](gogurt-linux-listener-host-systemduseradapter.md)

## Governing policies

- <a id="pa-9b26860c7f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter.process_is_running`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b5686a28069dcb481e8552be13c7dee98e769004565fec9aa5871b9c7a85828 -->

```json
{
  "contract": {
    "kind": "staticmethod",
    "signature": "\"(pid: 'int') -> 'bool'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "process_is_running",
  "owner": "gogurt_linux_listener_host.SystemdUserAdapter",
  "unit": "member"
}
```
