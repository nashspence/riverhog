# gogurt_core.PathInput

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-pathinput:6512394a40 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7c96f36b6"></a>
| Field | Shape |
|---|---|
| <a id="s-37bf4fbb0b"></a>`contract` | type="types.UnionType"; additional keys=`kind` |
| <a id="s-1b864c8d6d"></a>`distribution` | "gogurt-core" |
| <a id="s-0422a00dff"></a>`module` | "gogurt_core" |
| <a id="s-638e2d45e1"></a>`name` | "PathInput" |
| <a id="s-f5f0b89883"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9d1df8b3c5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.PathInput`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5cd7f39da4db91c735789fa027408787bcaaa4ee9a92f67b565aa602449891a -->

```json
{
  "contract": {
    "kind": "object",
    "type": "types.UnionType"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "PathInput",
  "unit": "export"
}
```
