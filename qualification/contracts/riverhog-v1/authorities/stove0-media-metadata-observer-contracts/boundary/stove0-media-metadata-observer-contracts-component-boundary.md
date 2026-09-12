# stove0-media-metadata-observer-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-5b9f85c41b:8900cf2c22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-c9cb2f894baf) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d6bab26ea4f8"></a>
| Field | Shape |
|---|---|
| <a id="s-46c8e0ec81a9"></a>`console_scripts` | empty object |
| <a id="s-efdf5c93cd8b"></a>`dependencies` | ["stove0-observer-protocol"] |
| <a id="s-975c7a7abb53"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-c24249eec25b"></a>`optional_dependencies` | empty object |
| <a id="s-875ab8bbd623"></a>`path` | "reference/stove0/observers/contracts/media-metadata" |
| <a id="s-ba43191f0ceb"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-d1e06320d2c8"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/42`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08ccde38e7b0d13fbacd1c26c58068780224373b88950d673a3e723d96349337 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "stove0-observer-protocol"
  ],
  "distribution": "stove0-media-metadata-observer-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/observers/contracts/media-metadata",
  "role": "reference_component"
}
```
