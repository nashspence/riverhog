# gogurt_core.GOGURT_EMOJI

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurt-emoji:04a5e9d37c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8721821351"></a>
| Field | Shape |
|---|---|
| <a id="s-9467f233a0"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-4856efb1be"></a>`distribution` | "gogurt-core" |
| <a id="s-7368c70cc3"></a>`module` | "gogurt_core" |
| <a id="s-28ea9d6bc8"></a>`name` | "GOGURT_EMOJI" |
| <a id="s-2c68c4ef99"></a>`unit` | "export" |

## Governing policies

- <a id="pa-92e17de71a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GOGURT_EMOJI`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f13a6cc99c330546502ca4bea815ca2f5fc73f7416f26d6ea950387fa7ef22e3 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "🛹"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GOGURT_EMOJI",
  "unit": "export"
}
```
