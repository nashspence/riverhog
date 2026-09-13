# gogurt_core.execute_gogurt_action

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-execute-gogurt-action:634f219f85 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d7c150f5b"></a>
| Field | Shape |
|---|---|
| <a id="s-51abc1b662"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-81f67d4ff5"></a>`distribution` | "gogurt-core" |
| <a id="s-b9df29100f"></a>`module` | "gogurt_core" |
| <a id="s-c732b4836d"></a>`name` | "execute_gogurt_action" |
| <a id="s-a26bd4c886"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a87e5d3877"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.execute_gogurt_action`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa0bd2544e26d80cb39304047f256097ee049a0f354954f1aa45fcbc8c0dbccb -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plan: 'Mapping[str, object]', *, provider: 'MountedVolumeProvider', capture_output: 'bool' = False) -> 'subprocess.CompletedProcess[str]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "execute_gogurt_action",
  "unit": "export"
}
```
