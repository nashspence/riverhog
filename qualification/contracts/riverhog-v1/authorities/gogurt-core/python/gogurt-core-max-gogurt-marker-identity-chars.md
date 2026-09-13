# gogurt_core.MAX_GOGURT_MARKER_IDENTITY_CHARS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-max-gogurt-marker-identity-chars:a130975cef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e3287a5a2e"></a>
| Field | Shape |
|---|---|
| <a id="s-279efec2d2"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-9c2dfc7f6f"></a>`distribution` | "gogurt-core" |
| <a id="s-4c710b0559"></a>`module` | "gogurt_core" |
| <a id="s-688459ac97"></a>`name` | "MAX_GOGURT_MARKER_IDENTITY_CHARS" |
| <a id="s-0fba0257cb"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b0370d47f6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.MAX_GOGURT_MARKER_IDENTITY_CHARS`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f115282f04346deb77e0554ac825986a3100b2b74e8c94b8c61ad762b8c7817 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 1024
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "MAX_GOGURT_MARKER_IDENTITY_CHARS",
  "unit": "export"
}
```
