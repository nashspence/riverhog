# riverhog-provenance-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-contracts:riverhog-provenance-contracts-component-boundary:6aca73c088 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-1055d00121) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-401fe0f86e"></a>
| Field | Shape |
|---|---|
| <a id="s-adfc87bf91"></a>`console_scripts` | empty object |
| <a id="s-f30570525d"></a>`dependencies` | [] |
| <a id="s-8194d802c5"></a>`distribution` | "riverhog-provenance-contracts" |
| <a id="s-7c43519657"></a>`optional_dependencies` | empty object |
| <a id="s-1adc49d434"></a>`path` | "packages/riverhog-provenance-contracts" |
| <a id="s-83d3c597bc"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-3dbdc1c63d"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/9`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 080d2dd485fdb63a71cb0353bc89990b80cf8e951ff217e300e0c3b0ac752637 -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "riverhog-provenance-contracts",
  "optional_dependencies": {},
  "path": "packages/riverhog-provenance-contracts",
  "role": "reusable_library"
}
```
