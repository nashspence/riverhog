# Compatibility: archive

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-archive:3eeae311a6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="p-915b8756ae"></a>

| Field | Value |
|---|---|
| <a id="s-484612c67b"></a>`archive` | `"A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises."` |

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/compatibility/archive`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12131fa17ee13c3795532979ccfd8b5095c8239671134270909a68afdff5308a -->

```json
"A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises."
```

</details>
