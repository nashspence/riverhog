# time-formats component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:time-formats:time-formats-component-boundary:b09c1ef5eb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-ddacb3fdac) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a1902da8e9"></a>
| Field | Shape |
|---|---|
| <a id="s-4f2353cfc1"></a>`console_scripts` | empty object |
| <a id="s-d4aaaa6669"></a>`dependencies` | [] |
| <a id="s-6f89afe276"></a>`distribution` | "time-formats" |
| <a id="s-46cf3ffa33"></a>`optional_dependencies` | empty object |
| <a id="s-45d72032b6"></a>`path` | "packages/time-formats" |
| <a id="s-f896b8a15f"></a>`role` | "internal_build_unit" |

## Governing policies

- <a id="pa-a62a66024e"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/14`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7af59e30a3a75be2fd9ddc30d73e4f3a8e6ace25b88fe9409cfddfb81c60e48 -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "time-formats",
  "optional_dependencies": {},
  "path": "packages/time-formats",
  "role": "internal_build_unit"
}
```
