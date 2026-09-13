# gogurt_listener_runtime.ListenerPlatformError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerplatformerror:13c4c0ad9d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e3416601f"></a>
| Field | Shape |
|---|---|
| <a id="s-7e0016d2f6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-36bb939134"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-096afebc16"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-3bb4f7c1a8"></a>`name` | "ListenerPlatformError" |
| <a id="s-88988e7a79"></a>`unit` | "export" |

## Governing policies

- <a id="pa-340ec15c3f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerPlatformError`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4622e44e84f7c9f692b4304eaa64748a24a9a26f183d471a16eb1d55260b3ea7 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ListenerPlatformError",
  "unit": "export"
}
```
