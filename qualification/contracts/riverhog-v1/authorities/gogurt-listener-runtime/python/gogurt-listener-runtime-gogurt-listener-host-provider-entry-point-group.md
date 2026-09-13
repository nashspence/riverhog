# gogurt_listener_runtime.GOGURT_LISTENER_HOST_PROVIDER_ENTRY_POINT_GROUP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-gogurt-listener-h-a3f26ce64b:53a1e91c5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d23c22e3a8"></a>
| Field | Shape |
|---|---|
| <a id="s-c915672fd7"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-00c9459772"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-96e37d8cd4"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-fa6dde2822"></a>`name` | "GOGURT_LISTENER_HOST_PROVIDER_ENTRY_POINT_GROUP" |
| <a id="s-b0e247f8b7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6ad185d19a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.GOGURT_LISTENER_HOST_PROVIDER_ENTRY_POINT_GROUP`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4816e18e94d900144bd1f4455340f0473f56fa288e05bfefecceb392bcdc14f7 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt.listener-host-providers"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "GOGURT_LISTENER_HOST_PROVIDER_ENTRY_POINT_GROUP",
  "unit": "export"
}
```
