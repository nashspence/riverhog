# config-validation component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:config-validation:config-validation-component-boundary:dcc8a2932b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [config-validation](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-49579f7dfe1c) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d742e86a466a"></a>
| Field | Shape |
|---|---|
| <a id="s-b872373fea40"></a>`console_scripts` | empty object |
| <a id="s-a514accd30f1"></a>`dependencies` | [] |
| <a id="s-184c9e98c4b1"></a>`distribution` | "config-validation" |
| <a id="s-d9a4759e62a9"></a>`optional_dependencies` | empty object |
| <a id="s-4913a62e2251"></a>`path` | "packages/config-validation" |
| <a id="s-e7a3d24d9a2f"></a>`role` | "internal_build_unit" |

## Governing policies

- <a id="pa-9846377ffa6b"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b43a98542be86ea9ac8eadce3ce534ade6681802cc05dda9bd5f9062cf4ccfe3 -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "config-validation",
  "optional_dependencies": {},
  "path": "packages/config-validation",
  "role": "internal_build_unit"
}
```
