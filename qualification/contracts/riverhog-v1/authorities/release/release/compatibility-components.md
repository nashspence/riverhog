# Compatibility: components

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-components:3172f9163b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: "A supported deployment runs components from one coordinated product version."

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/components`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2f0f3bfa6f92cd063f8130be900cbf2e446f38b94c0b918ea0fa6c36c24e295 -->

```json
"A supported deployment runs components from one coordinated product version."
```
