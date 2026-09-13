# gogurt_core.validate_gogurt_action_executables

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-validate-gogurt-action-executables:87d57270b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-462a1fbb34"></a>
| Field | Shape |
|---|---|
| <a id="s-61ba7ea504"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-370d4f7314"></a>`distribution` | "gogurt-core" |
| <a id="s-42506be97a"></a>`module` | "gogurt_core" |
| <a id="s-17c1fdf842"></a>`name` | "validate_gogurt_action_executables" |
| <a id="s-cfea822f5e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-7177ae7989"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.validate_gogurt_action_executables`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c928e052d6e9371956b26f523f5fdaf7b3296f24308fef5336a3c60fb52f700 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_file: 'PathInput', *, actions_dir: 'PathInput | None' = None) -> 'list[GogurtAction]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "validate_gogurt_action_executables",
  "unit": "export"
}
```
