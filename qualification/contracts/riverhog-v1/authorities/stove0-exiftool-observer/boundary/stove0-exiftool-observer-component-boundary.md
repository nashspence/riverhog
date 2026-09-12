# stove0-exiftool-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-exiftool-observer:stove0-exiftool-observer-component-boundary:351c5bf99b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-b10c23be612d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cfea4322176c"></a>
| Field | Shape |
|---|---|
| <a id="s-43ae1198a68b"></a>`console_scripts` | additional keys=`stove0-exiftool-observer` |
| <a id="s-d2680be15136"></a>`dependencies` | ["http-api-contracts","stove0-media-metadata-observer-contracts","stove0-observer-protocol","stove0-observer-support"] |
| <a id="s-97b87960e252"></a>`distribution` | "stove0-exiftool-observer" |
| <a id="s-e2f77352fecd"></a>`optional_dependencies` | empty object |
| <a id="s-35c82ed39f33"></a>`path` | "reference/stove0/observers/exiftool" |
| <a id="s-a6c34d7c66d8"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-75eb8c4e384f"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/44`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 761f99f35b6dce0d9e7f34864664c64de99867f2e86b08c70fdbfbe48c0d8ec0 -->

```json
{
  "console_scripts": {
    "stove0-exiftool-observer": "stove0_exiftool_observer.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "stove0-media-metadata-observer-contracts",
    "stove0-observer-protocol",
    "stove0-observer-support"
  ],
  "distribution": "stove0-exiftool-observer",
  "optional_dependencies": {},
  "path": "reference/stove0/observers/exiftool",
  "role": "reference_component"
}
```
