# Compatibility: recovery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-recovery:916e8684a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="s-0680f26670"></a>
- Shape: "Every later v1 recovery release reads every valid earlier v1 archive and provenance set."

## Governing policies

- <a id="pa-d1a9500a21"></a>[compatibility/recovery/v1](../../../policies/index.md#p-04aa4508f1)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/recovery`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f7f0f48d479c3063f37943c291f033ea396284b3228af54b185b755b0b3e968 -->

```json
"Every later v1 recovery release reads every valid earlier v1 archive and provenance set."
```
