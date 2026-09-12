# gogurt-windows-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-windows-mounted-volume:gogurt-windows-mounted-volume-component-boundary:45fa244e43 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-mounted-volume](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-5edeb55cc6) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-92fdb58906"></a>
| Field | Shape |
|---|---|
| <a id="s-acd080a0e4"></a>`console_scripts` | empty object |
| <a id="s-24956f95e9"></a>`dependencies` | ["gogurt-core","gogurt-path-volume-support"] |
| <a id="s-7470e75e3c"></a>`distribution` | "gogurt-windows-mounted-volume" |
| <a id="s-799255d55d"></a>`optional_dependencies` | empty object |
| <a id="s-ebfe4e19b4"></a>`path` | "reference/gogurt/mounted-volume/windows" |
| <a id="s-63143ff0eb"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-4539bec2b4"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/22`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffe1b5f6da03b41a6f4e05633dbd3e31f7a6d468997353cd89a52998349ac72e -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-core",
    "gogurt-path-volume-support"
  ],
  "distribution": "gogurt-windows-mounted-volume",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/windows",
  "role": "reference_component"
}
```
