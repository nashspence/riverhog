# gogurt_core.load_gogurt_actions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-load-gogurt-actions:7ebd716ee2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-046d6794b2"></a>
| Field | Shape |
|---|---|
| <a id="s-1a7334f710"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3677eccae3"></a>`distribution` | "gogurt-core" |
| <a id="s-00cb030ea1"></a>`module` | "gogurt_core" |
| <a id="s-7f5a9d7f1e"></a>`name` | "load_gogurt_actions" |
| <a id="s-0b2ddce4f4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-bf31d27195"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.load_gogurt_actions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ca93e7094d7bc28edd88d502be6bda9a6a46df6d648df1746a8978c9cb4883b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_file: 'PathInput') -> 'list[GogurtAction]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "load_gogurt_actions",
  "unit": "export"
}
```
