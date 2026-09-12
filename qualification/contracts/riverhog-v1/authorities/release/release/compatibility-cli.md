# Compatibility: cli

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-cli:3dc2cd12af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [compatibility](index.md#f-6df58a8f93) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-11a2acfae8"></a>
- Shape: "Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people."

## Governing policies

- <a id="pa-fce4b7d691"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/cli`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e729f79071583e4a90de990d6f8b6da8a2e7eb3dbd228a7383834c7f95993d70 -->

```json
"Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people."
```
