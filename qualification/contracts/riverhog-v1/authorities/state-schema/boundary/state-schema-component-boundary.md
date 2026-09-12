# state-schema component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:state-schema:state-schema-component-boundary:a60d84df07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-d4db84c7f0) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1fb6f2538a"></a>
| Field | Shape |
|---|---|
| <a id="s-28bb885e4f"></a>`console_scripts` | empty object |
| <a id="s-478de72842"></a>`dependencies` | [] |
| <a id="s-9d0450f73b"></a>`distribution` | "state-schema" |
| <a id="s-bff02a1709"></a>`optional_dependencies` | empty object |
| <a id="s-88b2d73edf"></a>`path` | "packages/state-schema" |
| <a id="s-7e98300581"></a>`role` | "internal_build_unit" |

## Governing policies

- <a id="pa-1a1d46f6f7"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/13`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f77d2a929a73da9a7532ca1c1624c0e6c2257c2cbd33caf5c3fcc026ea80a50d -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "state-schema",
  "optional_dependencies": {},
  "path": "packages/state-schema",
  "role": "internal_build_unit"
}
```
