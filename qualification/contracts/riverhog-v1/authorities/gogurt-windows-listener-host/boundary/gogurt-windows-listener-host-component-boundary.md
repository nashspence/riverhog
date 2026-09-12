# gogurt-windows-listener-host component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-windows-listener-host:gogurt-windows-listener-host-component-boundary:ec24a040b4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-13778a9bd7) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-658284c2e3"></a>
| Field | Shape |
|---|---|
| <a id="s-cede8350e5"></a>`console_scripts` | empty object |
| <a id="s-d6ca8a0cd9"></a>`dependencies` | ["gogurt-listener-runtime"] |
| <a id="s-764f4a12df"></a>`distribution` | "gogurt-windows-listener-host" |
| <a id="s-7f8ef58b4b"></a>`optional_dependencies` | empty object |
| <a id="s-d0fbe7a17e"></a>`path` | "reference/gogurt/listener-host/windows" |
| <a id="s-066811d39c"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-6a7cab0a6a"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/18`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8441da71f35f095e333b012c26c2604449eccde2e609b6a64717285fccd15e14 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt-windows-listener-host",
  "optional_dependencies": {},
  "path": "reference/gogurt/listener-host/windows",
  "role": "reference_component"
}
```
