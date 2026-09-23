# Compatibility: recovery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-recovery:916e8684a0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="p-04aa4508f1"></a>

| Field | Value |
|---|---|
| <a id="s-0680f26670"></a>`recovery` | `"Every later v1 recovery release reads every valid earlier v1 archive and provenance set."` |

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/compatibility/recovery`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f7f0f48d479c3063f37943c291f033ea396284b3228af54b185b755b0b3e968 -->

```json
"Every later v1 recovery release reads every valid earlier v1 archive and provenance set."
```

</details>
