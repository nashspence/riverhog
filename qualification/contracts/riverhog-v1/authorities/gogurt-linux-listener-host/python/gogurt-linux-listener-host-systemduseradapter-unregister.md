# gogurt_linux_listener_host.SystemdUserAdapter.unregister

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduserada-731f75eea4:cf31dc33ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cea2f065e5"></a>
| Field | Shape |
|---|---|
| <a id="s-fa5a92fc21"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-18ec91ea9c"></a>`distribution` | "gogurt-linux-listener-host" |
| <a id="s-15e6af8e38"></a>`module` | "gogurt_linux_listener_host" |
| <a id="s-0aa22f4aa0"></a>`name` | "unregister" |
| <a id="s-e86f625c25"></a>`owner` | "gogurt_linux_listener_host.SystemdUserAdapter" |
| <a id="s-63cbc3bc9d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_linux_listener_host.SystemdUserAdapter](gogurt-linux-listener-host-systemduseradapter.md)

## Governing policies

- <a id="pa-4b1280f913"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter.unregister`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1b9d404a9611fda40919559abcdcfeba5cce3c0fe1fd2a73bba7a45ca6110e6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "unregister",
  "owner": "gogurt_linux_listener_host.SystemdUserAdapter",
  "unit": "member"
}
```
