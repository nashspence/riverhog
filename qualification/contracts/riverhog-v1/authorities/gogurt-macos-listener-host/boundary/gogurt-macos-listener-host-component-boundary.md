# gogurt-macos-listener-host component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-macos-listener-host:gogurt-macos-listener-host-component-boundary:6f9cd3cc91 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-a6cf85fd61) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9860cf9dc1"></a>
| Field | Shape |
|---|---|
| <a id="s-e0fbc5a6b1"></a>`console_scripts` | empty object |
| <a id="s-391b58559b"></a>`dependencies` | ["gogurt-listener-runtime"] |
| <a id="s-57cd1a440c"></a>`distribution` | "gogurt-macos-listener-host" |
| <a id="s-7e90d49ca8"></a>`optional_dependencies` | empty object |
| <a id="s-ce9d841b76"></a>`path` | "reference/gogurt/listener-host/macos" |
| <a id="s-5bbb085960"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-1d4391fc0e"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/17`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6ee48c6d7241661ed76d2281ee69acc454552cbd91bef0641dd35ff593f2cdf -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt-macos-listener-host",
  "optional_dependencies": {},
  "path": "reference/gogurt/listener-host/macos",
  "role": "reference_component"
}
```
