# Python distribution: gogurt-path-volume-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-gogurt-path-volume-support:0f04457fc5 -->

Optional nonnormative path-mounted-volume support for Gogurt reference providers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-970e03193f"></a>
| Concern | Contract |
|---|---|
| <a id="s-f03078e187"></a>`artifacts` | [{"coordinate": "dist/gogurt_path_volume_support-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_path_volume_support-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-011564d47e"></a>`channel` | github-release |
| <a id="s-6fba35ead7"></a>`description` | Optional nonnormative path-mounted-volume support for Gogurt reference providers. |
| <a id="s-1bfa711b80"></a>`requires_python` | >=3.12 |
| <a id="s-0e52d9bd37"></a>`role` | reference_component |
| <a id="s-3701eb1b30"></a>`source` | reference/gogurt/mounted-volume/path-support/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-path-volume-support](../../../evidence/relationships.md#rn-1b4c9bf55f)

## Governing policies

- <a id="pa-0453f97cd7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-path-volume-support](../../../evidence/sources.md#src-fc01fccb44) — `reference/gogurt/mounted-volume/path-support/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-path-volume-support`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb51769dd1a1f205a85b1b853eef250b5ba434430297b812701ab4684161f726 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_path_volume_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_path_volume_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative path-mounted-volume support for Gogurt reference providers.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/mounted-volume/path-support/pyproject.toml"
}
```
