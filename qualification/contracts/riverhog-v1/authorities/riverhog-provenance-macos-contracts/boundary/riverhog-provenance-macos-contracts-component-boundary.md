# riverhog-provenance-macos-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-macos-contracts:riverhog-provenance-macos-contracts-compo-c34f47d4fe:154eb6c610 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-3716fce0c7ac) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-718a422e1e98"></a>
| Field | Shape |
|---|---|
| <a id="s-c91d42142f81"></a>`console_scripts` | empty object |
| <a id="s-dc30c56149e2"></a>`dependencies` | ["riverhog-provenance-contracts"] |
| <a id="s-2f510193f595"></a>`distribution` | "riverhog-provenance-macos-contracts" |
| <a id="s-96b5db97ac36"></a>`optional_dependencies` | empty object |
| <a id="s-c76d8ac68c62"></a>`path` | "reference/riverhog/provenance/contracts/macos" |
| <a id="s-224ad1a53e89"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-351a5f01702f"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/30`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b47c7c470d15d72f142a43764baae8d95a3c44d742dfb33bdb0f97ae9e1ee797 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance-macos-contracts",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/contracts/macos",
  "role": "reference_component"
}
```
