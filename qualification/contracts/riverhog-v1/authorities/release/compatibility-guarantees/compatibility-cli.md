# Compatibility: cli

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-cli:4725926ddf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="p-48a89776de"></a>
[Indexed applications](../../../policies/compatibility-cli-v1/applications.md)


| Field | Value |
|---|---|
| <a id="s-11a2acfae8"></a>`cli` | `"Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people."` |

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/compatibility/cli`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e729f79071583e4a90de990d6f8b6da8a2e7eb3dbd228a7383834c7f95993d70 -->

```json
"Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people."
```

</details>
