# Compatibility: cli

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-cli:3dc2cd12af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: "Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people."

## Governing policies

- `compatibility/cli/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/cli`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e729f79071583e4a90de990d6f8b6da8a2e7eb3dbd228a7383834c7f95993d70 -->

```json
"Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people."
```
