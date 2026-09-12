# stove0-target-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-support:stove0-target-support-component-boundary:a106e9e41a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-e894e59a87) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-75ee482442"></a>
| Field | Shape |
|---|---|
| <a id="s-6e156e202b"></a>`console_scripts` | additional keys=`stove0-target-conformance`, `stove0-target-schemas` |
| <a id="s-79f7e1ad89"></a>`dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","stove0-target-client","stove0-target-protocol"] |
| <a id="s-6bdce0baa3"></a>`distribution` | "stove0-target-support" |
| <a id="s-4d87ce98e4"></a>`optional_dependencies` | empty object |
| <a id="s-b953fd1809"></a>`path` | "reference/stove0/packages/target-support" |
| <a id="s-748227ee49"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-678a0d5805"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/55`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a034d2c3c2fab7ca469e5eafe6b98ca16a4a97087a54d25aff3950e84366bc9 -->

```json
{
  "console_scripts": {
    "stove0-target-conformance": "stove0_target_support.conformance:main",
    "stove0-target-schemas": "stove0_target_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "stove0-target-client",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-target-support",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/target-support",
  "role": "reusable_library"
}
```
