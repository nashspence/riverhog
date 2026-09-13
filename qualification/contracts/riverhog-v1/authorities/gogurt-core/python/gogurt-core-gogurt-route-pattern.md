# gogurt_core.GOGURT_ROUTE_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurt-route-pattern:7b767a293d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a3c3125304"></a>
| Field | Shape |
|---|---|
| <a id="s-951ac4d976"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-5cbbd578e9"></a>`distribution` | "gogurt-core" |
| <a id="s-c236a9575f"></a>`module` | "gogurt_core" |
| <a id="s-149f10b1f4"></a>`name` | "GOGURT_ROUTE_PATTERN" |
| <a id="s-a5f63d7059"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f10b992477"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GOGURT_ROUTE_PATTERN`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6855169333d8e1b5b5ed964d388b55b73a7a8d65a6600fa4ab8feb415f724bd5 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[a-z0-9]\u0028?:[a-z0-9-]{0,61}[a-z0-9])?$"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GOGURT_ROUTE_PATTERN",
  "unit": "export"
}
```
