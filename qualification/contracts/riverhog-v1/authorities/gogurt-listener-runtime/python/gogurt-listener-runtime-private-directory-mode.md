# gogurt_listener_runtime.PRIVATE_DIRECTORY_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-private-directory-mode:7bf12b06f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-749919e863"></a>
| Field | Shape |
|---|---|
| <a id="s-7b739605dc"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-82f5b75302"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-5d4d332aac"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-041832e5a2"></a>`name` | "PRIVATE_DIRECTORY_MODE" |
| <a id="s-7cab615dea"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ee825ae37f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.PRIVATE_DIRECTORY_MODE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4f678930c5878511ce76d84477c8a2799d264e58afa850efeec022bc0163379 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 448
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "PRIVATE_DIRECTORY_MODE",
  "unit": "export"
}
```
