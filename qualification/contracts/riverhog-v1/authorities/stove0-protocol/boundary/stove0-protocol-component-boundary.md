# stove0-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-protocol:stove0-protocol-component-boundary:4c1f816eb1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-1e2a2943db15) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5ccdfaab930b"></a>
| Field | Shape |
|---|---|
| <a id="s-22a2ef135484"></a>`console_scripts` | empty object |
| <a id="s-a02124744e1a"></a>`dependencies` | ["riverhog-protocol"] |
| <a id="s-13a3ccec5338"></a>`distribution` | "stove0-protocol" |
| <a id="s-9e87fb430357"></a>`optional_dependencies` | empty object |
| <a id="s-3dd2444e20d2"></a>`path` | "reference/stove0/packages/protocol" |
| <a id="s-93f3f36209c0"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-456b5097101d"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/51`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f98eb3abcca65308b8f58d79cdfb5401b551d8cbe5c98361d71ea613c7350fcc -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-protocol"
  ],
  "distribution": "stove0-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/protocol",
  "role": "reusable_library"
}
```
