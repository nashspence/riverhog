# gogurt-linux-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-linux-mounted-volume:gogurt-linux-mounted-volume-component-boundary:e47d4ab54c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-mounted-volume](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-570faf2338) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f6e3b7af02"></a>
| Field | Shape |
|---|---|
| <a id="s-c696e62a66"></a>`console_scripts` | empty object |
| <a id="s-c95b4d9e84"></a>`dependencies` | ["gogurt-core","gogurt-path-volume-support"] |
| <a id="s-fe2bc68f0e"></a>`distribution` | "gogurt-linux-mounted-volume" |
| <a id="s-9ffc9b84f6"></a>`optional_dependencies` | empty object |
| <a id="s-7f43b8a0cf"></a>`path` | "reference/gogurt/mounted-volume/linux" |
| <a id="s-7a4cb9be63"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-ac64d35c5e"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/19`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e608e21ea12cffabb5daaa9bb85fd7e3748086bb8036acd780496862be03ab8 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-core",
    "gogurt-path-volume-support"
  ],
  "distribution": "gogurt-linux-mounted-volume",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/linux",
  "role": "reference_component"
}
```
