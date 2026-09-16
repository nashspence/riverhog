# Compatibility: licensing

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-licensing:4f3dd01b07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract


| Field | Value |
|---|---|
| <a id="s-db30c07708"></a>`licensing` | `"Each continuing first-party Python distribution and OCI repository published in v1 retains every SPDX license alternative granted when that coordinate first shipped in v1; withdrawing a baseline grant requires a new major version."` |

## Governing policies

- <a id="pa-6427ef79d9"></a>[compatibility/licensing/v1](../../../policies/index.md#p-c6988e244a)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/licensing`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f03ab2f424bc55a98e1454cd86b2492d94dc1e638f4efdfc468615da1f4fce88 -->

```json
"Each continuing first-party Python distribution and OCI repository published in v1 retains every SPDX license alternative granted when that coordinate first shipped in v1; withdrawing a baseline grant requires a new major version."
```

</details>
