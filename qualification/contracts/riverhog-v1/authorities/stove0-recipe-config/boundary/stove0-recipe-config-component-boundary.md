# stove0-recipe-config component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-recipe-config:stove0-recipe-config-component-boundary:32ce7b8e00 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-60338293d9dd) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4777ea5281e5"></a>
| Field | Shape |
|---|---|
| <a id="s-b10601caf614"></a>`console_scripts` | empty object |
| <a id="s-dee402700035"></a>`dependencies` | ["config-validation","stove0-protocol","stove0-target-protocol"] |
| <a id="s-21fb9330ceb5"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-8a92307b4894"></a>`optional_dependencies` | empty object |
| <a id="s-5e4e93595f72"></a>`path` | "reference/stove0/packages/recipe-config" |
| <a id="s-4349a01dd1e4"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-469f5bf2e45f"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/52`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcb99aa9a5163ec20103a21a86db6682be65a5af56e04cbba891dc8fe015cef1 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "config-validation",
    "stove0-protocol",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-recipe-config",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/recipe-config",
  "role": "reusable_library"
}
```
