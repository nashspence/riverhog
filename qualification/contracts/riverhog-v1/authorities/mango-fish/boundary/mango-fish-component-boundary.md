# mango-fish component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:mango-fish:mango-fish-component-boundary:23e5b8889e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-6b54119437) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f6f91b83ef"></a>
| Field | Shape |
|---|---|
| <a id="s-010073a0d2"></a>`console_scripts` | additional keys=`mango-fish` |
| <a id="s-8e18a00e21"></a>`dependencies` | ["lifecycle-events","state-schema"] |
| <a id="s-8a37451c42"></a>`distribution` | "mango-fish" |
| <a id="s-b18cfd3463"></a>`optional_dependencies` | empty object |
| <a id="s-081bbe85d4"></a>`path` | "reference/riverhog/applications/mango-fish" |
| <a id="s-b1740bfa51"></a>`role` | "reference_application" |

## Governing policies

- <a id="pa-334a08180a"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/25`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 113e1f07a2dadb12b61fe99c441ef6fe3a3fa0f3d7be453dccb6c61757b7a072 -->

```json
{
  "console_scripts": {
    "mango-fish": "mango_fish.cli:main"
  },
  "dependencies": [
    "lifecycle-events",
    "state-schema"
  ],
  "distribution": "mango-fish",
  "optional_dependencies": {},
  "path": "reference/riverhog/applications/mango-fish",
  "role": "reference_application"
}
```
