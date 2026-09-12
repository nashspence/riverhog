# riverhog-provenance-linux-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-linux-contracts:riverhog-provenance-linux-contracts-compo-9fff7df012:2a32b7762b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-e31193a0c7f5) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-556c4e15bcf5"></a>
| Field | Shape |
|---|---|
| <a id="s-c5cfa39b5f2d"></a>`console_scripts` | empty object |
| <a id="s-b126b0a0a0de"></a>`dependencies` | ["riverhog-provenance-contracts"] |
| <a id="s-1e40b5d5f53a"></a>`distribution` | "riverhog-provenance-linux-contracts" |
| <a id="s-590fd61da21a"></a>`optional_dependencies` | empty object |
| <a id="s-d0cc1ef83d96"></a>`path` | "reference/riverhog/provenance/contracts/linux" |
| <a id="s-65d73cbfb15c"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-c0bd6e8a15bf"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/29`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55a45b71147e2ad0c9d7766f7a12bb793971b867e2c09887990e17b3113798eb -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance-linux-contracts",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/contracts/linux",
  "role": "reference_component"
}
```
