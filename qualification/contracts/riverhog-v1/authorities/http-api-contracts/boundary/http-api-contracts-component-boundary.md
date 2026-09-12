# http-api-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:http-api-contracts:http-api-contracts-component-boundary:f712265b1f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-497b7f4ae42e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-92163e1a0e40"></a>
| Field | Shape |
|---|---|
| <a id="s-d411bd2453c6"></a>`console_scripts` | empty object |
| <a id="s-e14ce1698540"></a>`dependencies` | [] |
| <a id="s-f5c98573b787"></a>`distribution` | "http-api-contracts" |
| <a id="s-d64981da7aaa"></a>`optional_dependencies` | empty object |
| <a id="s-5ea42142bbc2"></a>`path` | "packages/http-api-contracts" |
| <a id="s-012e0e1aa998"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-5a3dcdc473e0"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 357534d34f65d1ee9ee0c71a933da003f11ecaca1db6d413c17490e4d4e92264 -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "http-api-contracts",
  "optional_dependencies": {},
  "path": "packages/http-api-contracts",
  "role": "reusable_library"
}
```
