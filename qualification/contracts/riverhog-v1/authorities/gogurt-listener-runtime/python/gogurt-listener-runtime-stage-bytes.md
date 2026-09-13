# gogurt_listener_runtime.stage_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-stage-bytes:43b51e0bae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-36292677f9"></a>
| Field | Shape |
|---|---|
| <a id="s-019ae5a557"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a4a98352c8"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-7ec6c31c51"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-1874271e48"></a>`name` | "stage_bytes" |
| <a id="s-78de8b662d"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6e59306290"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.stage_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44de47271dc6384d5f42b315e459a74786a00d3e7fe2d60bed5707ce43bec553 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(destination: 'Path', content: 'bytes', *, mode: 'int') -> 'Path'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "stage_bytes",
  "unit": "export"
}
```
