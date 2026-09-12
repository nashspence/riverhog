# riverhog-provenance-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-contracts:riverhog-provenance-contracts-component-boundary:6aca73c088 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-1055d001213d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-401fe0f86e07"></a>
| Field | Shape |
|---|---|
| <a id="s-adfc87bf91fb"></a>`console_scripts` | empty object |
| <a id="s-f30570525dc4"></a>`dependencies` | [] |
| <a id="s-8194d802c5f4"></a>`distribution` | "riverhog-provenance-contracts" |
| <a id="s-7c435196574f"></a>`optional_dependencies` | empty object |
| <a id="s-1adc49d434d4"></a>`path` | "packages/riverhog-provenance-contracts" |
| <a id="s-83d3c597bc36"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-3dbdc1c63dbe"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

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
