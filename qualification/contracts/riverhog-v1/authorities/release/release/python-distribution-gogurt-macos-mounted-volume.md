# Python distribution: gogurt-macos-mounted-volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-gogurt-macos-mounted-volume:d20b0a21e7 -->

Optional nonnormative macOS mounted-volume reference for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-9a95acaa83"></a>
| Concern | Contract |
|---|---|
| <a id="s-bd8eb6ba21"></a>`artifacts` | [{"coordinate": "dist/gogurt_macos_mounted_volume-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_macos_mounted_volume-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-4127876bfb"></a>`channel` | github-release |
| <a id="s-daac4b9373"></a>`description` | Optional nonnormative macOS mounted-volume reference for Gogurt. |
| <a id="s-6101c9b7b3"></a>`requires_python` | >=3.12 |
| <a id="s-edfbc33440"></a>`role` | reference_component |
| <a id="s-bceb97ae45"></a>`source` | reference/gogurt/mounted-volume/macos/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-macos-mounted-volume](../../../evidence/relationships.md#rn-36cc5f6cd2)

## Governing policies

- <a id="pa-2641244481"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-macos-mounted-volume](../../../evidence/sources.md#src-ea30a89c8b) — `reference/gogurt/mounted-volume/macos/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-macos-mounted-volume`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6b8b02f00cac5c610fe9a55fbe05b0dccb42a1ddf6865dce3b2a6c5f412234c -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_macos_mounted_volume-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_macos_mounted_volume-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative macOS mounted-volume reference for Gogurt.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/mounted-volume/macos/pyproject.toml"
}
```
