# riverhog-provenance-linux-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-linux-observer:riverhog-provenance-linux-observer-compon-61290e85ef:09854638c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-observer](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-feb67f0003) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-20aaed552e"></a>
| Field | Shape |
|---|---|
| <a id="s-f8aaebb8e5"></a>`console_scripts` | empty object |
| <a id="s-6c53a1bd32"></a>`dependencies` | ["riverhog-provenance","riverhog-provenance-linux-contracts"] |
| <a id="s-0e98e68f61"></a>`distribution` | "riverhog-provenance-linux-observer" |
| <a id="s-1af389d5aa"></a>`optional_dependencies` | empty object |
| <a id="s-8c4f28aa6d"></a>`path` | "reference/riverhog/provenance/observers/linux" |
| <a id="s-12efac68c5"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-54c57c95eb"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/32`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98771297bba0032b0230556687d61a8bb1ed608dad6b0ef4d7a477ea9bf8de53 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance",
    "riverhog-provenance-linux-contracts"
  ],
  "distribution": "riverhog-provenance-linux-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/linux",
  "role": "reference_component"
}
```
