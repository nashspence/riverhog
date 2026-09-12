# Compatibility: http api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-http-api:859d1abf1a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: "Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1."

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/http_api`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2585cfdddab6eb45045fb1e0a19776f07029a40553fc17b28811f710586512ac -->

```json
"Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1."
```
