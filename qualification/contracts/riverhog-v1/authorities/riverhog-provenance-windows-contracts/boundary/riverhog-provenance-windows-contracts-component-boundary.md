# riverhog-provenance-windows-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-windows-contracts:riverhog-provenance-windows-contracts-com-27ca113ded:d918bb9f73 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-d967ca917a) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-24b14260ad"></a>
| Field | Shape |
|---|---|
| <a id="s-c558b6f6c4"></a>`console_scripts` | empty object |
| <a id="s-8acff826ea"></a>`dependencies` | ["riverhog-provenance-contracts"] |
| <a id="s-535c8da088"></a>`distribution` | "riverhog-provenance-windows-contracts" |
| <a id="s-9947d30478"></a>`optional_dependencies` | empty object |
| <a id="s-8b8331b3e2"></a>`path` | "reference/riverhog/provenance/contracts/windows" |
| <a id="s-04d671ba74"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-b7c40f471c"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/31`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad1320843a2a320965505d05601c3bc58529d81a8b82441dfe70e520d3647a92 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance-windows-contracts",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/contracts/windows",
  "role": "reference_component"
}
```
