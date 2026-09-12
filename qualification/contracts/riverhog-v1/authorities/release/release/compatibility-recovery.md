# Compatibility: recovery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-recovery:b9527b16ee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [compatibility](index.md#f-6df58a8f932e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0680f26670e3"></a>
- Shape: "Every later v1 recovery release reads every valid earlier v1 archive and provenance set."

## Governing policies

- <a id="pa-d17abbfe5e25"></a>[compatibility/recovery/v1](../../../policies/index.md#p-04aa4508f139)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/recovery`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f7f0f48d479c3063f37943c291f033ea396284b3228af54b185b755b0b3e968 -->

```json
"Every later v1 recovery release reads every valid earlier v1 archive and provenance set."
```
