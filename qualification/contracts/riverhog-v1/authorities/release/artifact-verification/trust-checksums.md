# Trust: checksums

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: artifact-verification:release:trust-checksums:b0b7913722 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Artifact Verification](index.md) |

## External contract

<a id="s-fcad4b8eb1"></a>
| Concern | Contract |
|---|---|
| <a id="s-f6349a9ffc"></a>`coordinate` | `"SHA256SUMS"` |
| <a id="s-e3e3d48f48"></a>`scheme` | `"SHA-256"` |

## Governing policies

- <a id="pa-dcfc98cb62"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/trust/checksums`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41c102c212dbc3d5ff5c7229fcb066cbe3783757ec0883856ef838a95875190f -->

```json
{
  "coordinate": "SHA256SUMS",
  "scheme": "SHA-256"
}
```

</details>
