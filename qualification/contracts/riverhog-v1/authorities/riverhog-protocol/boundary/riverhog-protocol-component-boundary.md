# riverhog-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-protocol:riverhog-protocol-component-boundary:3737face3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-9d2acfbffb) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3edb81aa53"></a>
| Field | Shape |
|---|---|
| <a id="s-9235960704"></a>`console_scripts` | empty object |
| <a id="s-269932b1ea"></a>`dependencies` | ["http-api-contracts","lifecycle-events","riverhog-provenance-contracts"] |
| <a id="s-0e4bd183b1"></a>`distribution` | "riverhog-protocol" |
| <a id="s-3a00bd62a8"></a>`optional_dependencies` | empty object |
| <a id="s-e611de2384"></a>`path` | "packages/riverhog-protocol" |
| <a id="s-7b13b34599"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-fb4df5492b"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/7`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90b954e7efb6f9f8d9a0ed08063211e33ac04c9f992ac100ae80b01cacfc4c44 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "lifecycle-events",
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-protocol",
  "optional_dependencies": {},
  "path": "packages/riverhog-protocol",
  "role": "reusable_library"
}
```
