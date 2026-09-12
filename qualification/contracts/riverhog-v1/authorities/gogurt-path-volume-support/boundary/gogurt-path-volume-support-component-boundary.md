# gogurt-path-volume-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-path-volume-support:gogurt-path-volume-support-component-boundary:78929299bf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-f3303f2d14) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a0805602ee"></a>
| Field | Shape |
|---|---|
| <a id="s-19a9c12a9b"></a>`console_scripts` | empty object |
| <a id="s-e7939bae82"></a>`dependencies` | ["config-validation","gogurt-core"] |
| <a id="s-5614c698e5"></a>`distribution` | "gogurt-path-volume-support" |
| <a id="s-619987afab"></a>`optional_dependencies` | empty object |
| <a id="s-32d153874f"></a>`path` | "reference/gogurt/mounted-volume/path-support" |
| <a id="s-54c780731c"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-472bb52bf0"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/21`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41d7be3cea5bba8db0ae3711119e2a9fb2f5b3635c4450b39f1d07e74181657d -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "config-validation",
    "gogurt-core"
  ],
  "distribution": "gogurt-path-volume-support",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/path-support",
  "role": "reference_component"
}
```
