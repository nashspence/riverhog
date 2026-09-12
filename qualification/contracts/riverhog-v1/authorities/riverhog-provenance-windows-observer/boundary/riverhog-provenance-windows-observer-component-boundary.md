# riverhog-provenance-windows-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-windows-observer:riverhog-provenance-windows-observer-comp-f14e00eb8c:19842e4df0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-observer](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-48c09b70b8) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-44fefd3da0"></a>
| Field | Shape |
|---|---|
| <a id="s-39b1588c54"></a>`console_scripts` | empty object |
| <a id="s-c5214332c4"></a>`dependencies` | ["riverhog-provenance","riverhog-provenance-windows-contracts"] |
| <a id="s-c5b5d08831"></a>`distribution` | "riverhog-provenance-windows-observer" |
| <a id="s-e51ae7a95d"></a>`optional_dependencies` | empty object |
| <a id="s-47f0b275ea"></a>`path` | "reference/riverhog/provenance/observers/windows" |
| <a id="s-307bb3dd09"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-52ade92b7a"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/34`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6aa23de111a1bca534b3b3fa93e76add4beec70cce178419895befa744a1b8b9 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance",
    "riverhog-provenance-windows-contracts"
  ],
  "distribution": "riverhog-provenance-windows-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/windows",
  "role": "reference_component"
}
```
