# riverhog-archive-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-archive-contracts:riverhog-archive-contracts-component-boundary:8d0029d3a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-9534156645ee) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8229eafb99a3"></a>
| Field | Shape |
|---|---|
| <a id="s-1f321a02428a"></a>`console_scripts` | empty object |
| <a id="s-d486eef5b291"></a>`dependencies` | [] |
| <a id="s-5c499cfc6e03"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-ca82e46ca7b6"></a>`optional_dependencies` | empty object |
| <a id="s-2d0db4a4b2d6"></a>`path` | "packages/riverhog-archive-contracts" |
| <a id="s-79b16f5d0331"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-8bfd91d585ae"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0034421b90dec670862cab707c555184090fde2a5478db69424ad8e16678b9d -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "riverhog-archive-contracts",
  "optional_dependencies": {},
  "path": "packages/riverhog-archive-contracts",
  "role": "reusable_library"
}
```
