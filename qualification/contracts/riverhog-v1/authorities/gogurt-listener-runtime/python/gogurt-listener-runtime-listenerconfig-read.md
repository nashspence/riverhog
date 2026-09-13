# gogurt_listener_runtime.ListenerConfig.read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerconfig-read:ecaf14ded9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-093eacc82d"></a>
| Field | Shape |
|---|---|
| <a id="s-29a8185b46"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ff3d420a0e"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-b69b7ec7f9"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-a74dc5e95b"></a>`name` | "read" |
| <a id="s-1cc70af083"></a>`owner` | "gogurt_listener_runtime.ListenerConfig" |
| <a id="s-c6caa26d81"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_listener_runtime.ListenerConfig](gogurt-listener-runtime-listenerconfig.md)

## Governing policies

- <a id="pa-58cb450dfb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerConfig.read`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d4c978754aed240337c4f5ce0801239df736dce9e11c56a43b33ef10bdb9da0 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, path: 'Path') -> 'ListenerConfig'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "read",
  "owner": "gogurt_listener_runtime.ListenerConfig",
  "unit": "member"
}
```
