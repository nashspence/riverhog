# Compatibility: http api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-http-api:859d1abf1a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [compatibility](index.md#f-6df58a8f93) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9e2eb4b18c"></a>
- Shape: "Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1."

## Governing policies

- <a id="pa-2925ad0c5d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/http_api`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2585cfdddab6eb45045fb1e0a19776f07029a40553fc17b28811f710586512ac -->

```json
"Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1."
```
