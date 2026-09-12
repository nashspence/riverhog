# riverhog-provenance component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance:riverhog-provenance-component-boundary:b8addbec20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-e831ae2d08) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e45437c5d1"></a>
| Field | Shape |
|---|---|
| <a id="s-2c8960c747"></a>`console_scripts` | empty object |
| <a id="s-60d6e2688f"></a>`dependencies` | ["riverhog-provenance-contracts"] |
| <a id="s-1640a19711"></a>`distribution` | "riverhog-provenance" |
| <a id="s-0777b849a7"></a>`optional_dependencies` | empty object |
| <a id="s-0abe1e1ff7"></a>`path` | "packages/riverhog-provenance" |
| <a id="s-5d507eb1d9"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-89476d4520"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/8`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5fbd501af743c1d1bf20a1fdd4e410d8b73c65f41e1cbbfeadc5030ade31562 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance",
  "optional_dependencies": {},
  "path": "packages/riverhog-provenance",
  "role": "reusable_library"
}
```
