# gogurt-linux-listener-host component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-linux-listener-host:gogurt-linux-listener-host-component-boundary:d8ca97d4f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-a09c5964e1) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0356e78112"></a>
| Field | Shape |
|---|---|
| <a id="s-7b2c570f77"></a>`console_scripts` | empty object |
| <a id="s-371c7f47f6"></a>`dependencies` | ["gogurt-listener-runtime"] |
| <a id="s-a3a8b239cb"></a>`distribution` | "gogurt-linux-listener-host" |
| <a id="s-a650f56492"></a>`optional_dependencies` | empty object |
| <a id="s-e93a436f7d"></a>`path` | "reference/gogurt/listener-host/linux" |
| <a id="s-cb9c13c89e"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-5b28f77cf2"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/16`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6686206b45878ae5565f688a1515b7fd3597b6f7bde75adf69b3bd3b9af47f8 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt-linux-listener-host",
  "optional_dependencies": {},
  "path": "reference/gogurt/listener-host/linux",
  "role": "reference_component"
}
```
