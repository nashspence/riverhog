# gogurt_linux_listener_host.render_systemd_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-render-systemd-unit:73923df1d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cbd8173a9d"></a>
| Field | Shape |
|---|---|
| <a id="s-27adf249dc"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1e086694a1"></a>`distribution` | "gogurt-linux-listener-host" |
| <a id="s-823cc552f5"></a>`module` | "gogurt_linux_listener_host" |
| <a id="s-c0ca252a69"></a>`name` | "render_systemd_unit" |
| <a id="s-e770c1d8c2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-12cc1936f4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.render_systemd_unit`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2ec61e891e1c852333fa9f1d4be6e31598a78617fa8fe23b69b32cb34b771fa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(command: 'Sequence[str]') -> 'bytes'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "render_systemd_unit",
  "unit": "export"
}
```
