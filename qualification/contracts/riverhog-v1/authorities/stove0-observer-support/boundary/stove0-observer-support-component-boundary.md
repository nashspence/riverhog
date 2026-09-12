# stove0-observer-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-support:stove0-observer-support-component-boundary:8fe570167c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-2c3274444a28) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-170706122dad"></a>
| Field | Shape |
|---|---|
| <a id="s-9aec7dae76b7"></a>`console_scripts` | additional keys=`stove0-observer-conformance`, `stove0-observer-schemas` |
| <a id="s-bc081a7bda32"></a>`dependencies` | ["http-api-contracts","riverhog-client","stove0-observer-client","stove0-observer-protocol"] |
| <a id="s-2eab8e39807b"></a>`distribution` | "stove0-observer-support" |
| <a id="s-c6dedcda02b3"></a>`optional_dependencies` | empty object |
| <a id="s-0e928232a6f6"></a>`path` | "reference/stove0/packages/observer-support" |
| <a id="s-1b50d7ee14d7"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-fefd78459b9f"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/49`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76adbdbfbaa7c3989dbc9f0c557be0aa0b3a430876321d5bf9f4413a60f7ff41 -->

```json
{
  "console_scripts": {
    "stove0-observer-conformance": "stove0_observer_support.conformance:main",
    "stove0-observer-schemas": "stove0_observer_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "stove0-observer-client",
    "stove0-observer-protocol"
  ],
  "distribution": "stove0-observer-support",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/observer-support",
  "role": "reusable_library"
}
```
