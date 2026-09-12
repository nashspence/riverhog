# Compatibility: configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-configuration:ba0b4cc6a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [compatibility](index.md#f-6df58a8f93) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-43f1f247d9"></a>
- Shape: "Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected."

## Governing policies

- <a id="pa-e745bfc64a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/configuration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de93df3805314638224be1b8f0f4a7b30bd0b1d03e6c8b6938130f9b08122aec -->

```json
"Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected."
```
