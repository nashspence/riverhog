# Release artifact: installation:gogurt-listener-reference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release-artifacts:release:release-artifact-installation-gogurt-list-d18d93ca30:968ab8b60b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release Artifacts](index.md) |

## External contract

<a id="s-43811c1404"></a>
| Concern | Contract |
|---|---|
| <a id="s-9d9e49c3f7"></a>`coordinate` | `"gogurt-listener-v{version}.md"` |
| <a id="s-f11f401ad5"></a>`format` | `"markdown"` |

## Governing policies

- <a id="pa-7abfa68a08"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)

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

- `/external_contract/release/publication/release_artifacts/installation:gogurt-listener-reference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb4aea5e4f2ef707420bd091cb3a581fd4ef1c7eaaba19fb6c7d09aa0ea8d917 -->

```json
{
  "coordinate": "gogurt-listener-v{version}.md",
  "format": "markdown"
}
```

</details>
