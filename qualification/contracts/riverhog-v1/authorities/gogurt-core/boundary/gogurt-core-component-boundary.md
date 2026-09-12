# gogurt-core component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-core:gogurt-core-component-boundary:c502c632b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-44e0f36845) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e09844f7a4"></a>
| Field | Shape |
|---|---|
| <a id="s-65821fc342"></a>`console_scripts` | empty object |
| <a id="s-42f82a50df"></a>`dependencies` | ["config-validation"] |
| <a id="s-cc71e93fbd"></a>`distribution` | "gogurt-core" |
| <a id="s-1a74ecf2d2"></a>`optional_dependencies` | empty object |
| <a id="s-3a22f80089"></a>`path` | "reference/gogurt/packages/core" |
| <a id="s-88ba2a8423"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-f1bca67220"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/23`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a13dd0a99c9b2814ece9d86a5dfcb639be14fd3ccca9599d577f4d709cabb0cf -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "config-validation"
  ],
  "distribution": "gogurt-core",
  "optional_dependencies": {},
  "path": "reference/gogurt/packages/core",
  "role": "reusable_library"
}
```
