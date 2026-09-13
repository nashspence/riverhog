# gogurt_core.GOGURT_ROUTE_MARKER_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurt-route-marker-format:e688b90df1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a47df50168"></a>
| Field | Shape |
|---|---|
| <a id="s-cd676d4cf6"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-62ea6d7189"></a>`distribution` | "gogurt-core" |
| <a id="s-9179cf024e"></a>`module` | "gogurt_core" |
| <a id="s-3730404dfb"></a>`name` | "GOGURT_ROUTE_MARKER_FORMAT" |
| <a id="s-b046b54f92"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c2d6095125"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GOGURT_ROUTE_MARKER_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c288da89645a5b6e33a656486953d6cb731c49be12a794d604a03ca5e2df1bdb -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-route-marker/v1"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GOGURT_ROUTE_MARKER_FORMAT",
  "unit": "export"
}
```
