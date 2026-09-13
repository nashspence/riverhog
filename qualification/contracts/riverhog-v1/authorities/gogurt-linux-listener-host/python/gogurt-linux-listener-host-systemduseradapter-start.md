# gogurt_linux_listener_host.SystemdUserAdapter.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduseradapter-start:fd2a4497c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-017e9fc5fa"></a>
| Field | Shape |
|---|---|
| <a id="s-3bd9b3ae89"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4082bd68c9"></a>`distribution` | "gogurt-linux-listener-host" |
| <a id="s-82f0b93931"></a>`module` | "gogurt_linux_listener_host" |
| <a id="s-4531cba3df"></a>`name` | "start" |
| <a id="s-70cdced4c3"></a>`owner` | "gogurt_linux_listener_host.SystemdUserAdapter" |
| <a id="s-3c83f4af8c"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_linux_listener_host.SystemdUserAdapter](gogurt-linux-listener-host-systemduseradapter.md)

## Governing policies

- <a id="pa-d61094435f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter.start`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da938bc7fa6bb717686bca8ac5e0567230e828196f64e5b51ab88578d49ff178 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "start",
  "owner": "gogurt_linux_listener_host.SystemdUserAdapter",
  "unit": "member"
}
```
