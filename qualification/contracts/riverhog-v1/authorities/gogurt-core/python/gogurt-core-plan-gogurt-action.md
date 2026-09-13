# gogurt_core.plan_gogurt_action

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-plan-gogurt-action:f6099dc860 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f63babb7a0"></a>
| Field | Shape |
|---|---|
| <a id="s-2455052f2a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d75b490e5a"></a>`distribution` | "gogurt-core" |
| <a id="s-dbb9468a77"></a>`module` | "gogurt_core" |
| <a id="s-452a9dacf9"></a>`name` | "plan_gogurt_action" |
| <a id="s-bfad8acaa5"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ab47bc51b5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.plan_gogurt_action`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f8c6f64ed181541f26bed5cc90a32f94e503ed85ac696406da733ad93a79e90 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_file: 'PathInput', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', actions_dir: 'PathInput | None' = None) -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "plan_gogurt_action",
  "unit": "export"
}
```
