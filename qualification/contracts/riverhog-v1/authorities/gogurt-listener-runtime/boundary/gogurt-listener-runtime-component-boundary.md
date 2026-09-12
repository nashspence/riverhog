# gogurt-listener-runtime component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-listener-runtime:gogurt-listener-runtime-component-boundary:1a2ab8323b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-42748ff5a6) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-96efc60e3f"></a>
| Field | Shape |
|---|---|
| <a id="s-d0c35132f5"></a>`console_scripts` | empty object |
| <a id="s-3957e1e5cc"></a>`dependencies` | ["config-validation","gogurt-core"] |
| <a id="s-e8bac1c419"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-399b0ce340"></a>`optional_dependencies` | empty object |
| <a id="s-f6917758f1"></a>`path` | "reference/gogurt/packages/listener-runtime" |
| <a id="s-317054c508"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-2dcdaeaf80"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/24`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f32be8f7fbf756f1e2c5bdf891275e0852e3c691a6c5b7fad745e19a4f404b6 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "config-validation",
    "gogurt-core"
  ],
  "distribution": "gogurt-listener-runtime",
  "optional_dependencies": {},
  "path": "reference/gogurt/packages/listener-runtime",
  "role": "reusable_library"
}
```
