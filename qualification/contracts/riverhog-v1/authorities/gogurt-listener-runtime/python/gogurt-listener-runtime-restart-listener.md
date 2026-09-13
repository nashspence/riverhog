# gogurt_listener_runtime.restart_listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-restart-listener:57d97758c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d8d9fe57b3"></a>
| Field | Shape |
|---|---|
| <a id="s-31ba6a89cb"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ed98d0cd73"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-2706d20340"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-c1373dfbba"></a>`name` | "restart_listener" |
| <a id="s-4f49cab24f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-07c40dd468"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.restart_listener`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2fa349e63f4310e2919dfe32a8b02344d088f114dde53a43483d4de6c4534e61 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "restart_listener",
  "unit": "export"
}
```
