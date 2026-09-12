# lifecycle-events component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:lifecycle-events:lifecycle-events-component-boundary:dc11b5cdd5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-f27b4dd021) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f64f0387db"></a>
| Field | Shape |
|---|---|
| <a id="s-dcad65e4ac"></a>`console_scripts` | empty object |
| <a id="s-a3de31d020"></a>`dependencies` | ["time-formats"] |
| <a id="s-a6a2619bf1"></a>`distribution` | "lifecycle-events" |
| <a id="s-1676d7a301"></a>`optional_dependencies` | empty object |
| <a id="s-55cb291ddf"></a>`path` | "packages/lifecycle-events" |
| <a id="s-f789a38028"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-18499339de"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cfa9a09d7963dbd01e57766f30b10ed5f50d9882ddb3ecd5ec78d8af532b5cc8 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "time-formats"
  ],
  "distribution": "lifecycle-events",
  "optional_dependencies": {},
  "path": "packages/lifecycle-events",
  "role": "reusable_library"
}
```
