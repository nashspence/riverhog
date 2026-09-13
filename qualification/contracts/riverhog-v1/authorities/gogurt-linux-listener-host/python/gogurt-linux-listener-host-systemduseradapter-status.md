# gogurt_linux_listener_host.SystemdUserAdapter.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduseradapter-status:5849ba456d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a5786efa37"></a>
| Field | Shape |
|---|---|
| <a id="s-29dd2a4255"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-745d56619f"></a>`distribution` | "gogurt-linux-listener-host" |
| <a id="s-605c50b1e2"></a>`module` | "gogurt_linux_listener_host" |
| <a id="s-90fe141e90"></a>`name` | "status" |
| <a id="s-8ed525654d"></a>`owner` | "gogurt_linux_listener_host.SystemdUserAdapter" |
| <a id="s-7213d45ca9"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_linux_listener_host.SystemdUserAdapter](gogurt-linux-listener-host-systemduseradapter.md)

## Governing policies

- <a id="pa-384413af13"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter.status`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd693a9553d803312a41dacb76dda977553535c7586caff2f0a552ba92fc5577 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "status",
  "owner": "gogurt_linux_listener_host.SystemdUserAdapter",
  "unit": "member"
}
```
