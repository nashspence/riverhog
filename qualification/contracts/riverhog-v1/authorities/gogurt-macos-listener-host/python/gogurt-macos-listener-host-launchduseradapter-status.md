# gogurt_macos_listener_host.LaunchdUserAdapter.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-launchduseradapter-status:93df0c3772 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-71af6d5726"></a>
| Field | Shape |
|---|---|
| <a id="s-55914a2693"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4b043fedc7"></a>`distribution` | "gogurt-macos-listener-host" |
| <a id="s-38aac50322"></a>`module` | "gogurt_macos_listener_host" |
| <a id="s-52422148a3"></a>`name` | "status" |
| <a id="s-dda60cc189"></a>`owner` | "gogurt_macos_listener_host.LaunchdUserAdapter" |
| <a id="s-42327fe276"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_macos_listener_host.LaunchdUserAdapter](gogurt-macos-listener-host-launchduseradapter.md)

## Governing policies

- <a id="pa-3820e92e9a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.LaunchdUserAdapter.status`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88987b4bab9456488a8745b3d66b57096ac24c6b05d0249cf060da0cf4ac7cc6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "status",
  "owner": "gogurt_macos_listener_host.LaunchdUserAdapter",
  "unit": "member"
}
```
