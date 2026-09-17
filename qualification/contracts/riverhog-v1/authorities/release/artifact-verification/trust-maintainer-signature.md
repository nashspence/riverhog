# Trust: maintainer_signature

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: artifact-verification:release:trust-maintainer-signature:b67877bbd1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Artifact Verification](index.md) |

## External contract

<a id="s-8b0a3afb0d"></a>
| Concern | Contract |
|---|---|
| <a id="s-6cd01a4303"></a>`coordinate` | `"SHA256SUMS.minisig"` |
| <a id="s-605c054266"></a>`public_key_distribution` | `"The public key is distributed in the v1 documentation and every GitHub release."` |
| <a id="s-3a07c8929a"></a>`scheme` | `"minisign"` |

## Governing policies

- <a id="pa-d9ed0ca2ea"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)

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

- `/external_contract/release/publication/trust/maintainer_signature`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3cf0cff5b89420167c35f6f5e3e56a0ad0e8a3fd1544df8a6c18e88bc117d35 -->

```json
{
  "coordinate": "SHA256SUMS.minisig",
  "public_key_distribution": "The public key is distributed in the v1 documentation and every GitHub release.",
  "scheme": "minisign"
}
```

</details>
