# Compatibility: configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-configuration:75542d5f5b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="p-8dc08bb461"></a>
[Where this policy applies](../../../policies/compatibility-configuration-v1/applications.md)


| Field | Value |
|---|---|
| <a id="s-43f1f247d9"></a>`configuration` | `"Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected."` |

## Governing policies

- <a id="pa-8e2b7b9b16"></a>[compatibility/configuration/v1](#p-8dc08bb461)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/compatibility/configuration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de93df3805314638224be1b8f0f4a7b30bd0b1d03e6c8b6938130f9b08122aec -->

```json
"Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected."
```

</details>
