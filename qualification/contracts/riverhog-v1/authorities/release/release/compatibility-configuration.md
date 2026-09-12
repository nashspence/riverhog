# Compatibility: configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-configuration:ba0b4cc6a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: "Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected."

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/configuration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de93df3805314638224be1b8f0f4a7b30bd0b1d03e6c8b6938130f9b08122aec -->

```json
"Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected."
```
