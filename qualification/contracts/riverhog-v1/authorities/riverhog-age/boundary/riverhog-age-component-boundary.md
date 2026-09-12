# riverhog-age component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-age:riverhog-age-component-boundary:9702b630dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-366cc312dc11) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ab7517db5986"></a>
| Field | Shape |
|---|---|
| <a id="s-015a264cbf27"></a>`console_scripts` | empty object |
| <a id="s-36568c6fb193"></a>`dependencies` | [] |
| <a id="s-0b2e11d4127a"></a>`distribution` | "riverhog-age" |
| <a id="s-f604cfe07efa"></a>`optional_dependencies` | empty object |
| <a id="s-378540bebf96"></a>`path` | "packages/riverhog-age" |
| <a id="s-0ab84b985599"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-42cdc734924d"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a35c65ff8f57497522afa0971bbc89d8f634cac95720267a34c80d37c785e8fa -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "riverhog-age",
  "optional_dependencies": {},
  "path": "packages/riverhog-age",
  "role": "reusable_library"
}
```
