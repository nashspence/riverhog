# Compatibility: recovery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-recovery:b9527b16ee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: "Every later v1 recovery release reads every valid earlier v1 archive and provenance set."

## Governing policies

- `compatibility/recovery/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/recovery`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f7f0f48d479c3063f37943c291f033ea396284b3228af54b185b755b0b3e968 -->

```json
"Every later v1 recovery release reads every valid earlier v1 archive and provenance set."
```
