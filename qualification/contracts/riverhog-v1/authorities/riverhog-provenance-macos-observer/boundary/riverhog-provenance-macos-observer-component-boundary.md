# riverhog-provenance-macos-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-macos-observer:riverhog-provenance-macos-observer-compon-f6fe932ece:958fc43472 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-observer](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-e52e61da9e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-13ac926797"></a>
| Field | Shape |
|---|---|
| <a id="s-e3393bbb06"></a>`console_scripts` | empty object |
| <a id="s-e5f3a5e01b"></a>`dependencies` | ["riverhog-provenance","riverhog-provenance-macos-contracts"] |
| <a id="s-ea80c801b4"></a>`distribution` | "riverhog-provenance-macos-observer" |
| <a id="s-fb42c4b086"></a>`optional_dependencies` | empty object |
| <a id="s-f127efb7d7"></a>`path` | "reference/riverhog/provenance/observers/macos" |
| <a id="s-1ed6717f46"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-f22d7310b3"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/33`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a28689f84efafe5040c4dd714ea3b438ba0ece129602437923f37bd6c969a23 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance",
    "riverhog-provenance-macos-contracts"
  ],
  "distribution": "riverhog-provenance-macos-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/macos",
  "role": "reference_component"
}
```
