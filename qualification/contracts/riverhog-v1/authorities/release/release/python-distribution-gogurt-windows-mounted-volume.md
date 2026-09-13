# Python distribution: gogurt-windows-mounted-volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-gogurt-windows-mounted-volume:b9d85dac2b -->

Optional nonnormative Windows mounted-volume reference for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-b7258d7665"></a>
| Concern | Contract |
|---|---|
| <a id="s-7b850c7a35"></a>`artifacts` | [{"coordinate": "dist/gogurt_windows_mounted_volume-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_windows_mounted_volume-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-038b765868"></a>`channel` | github-release |
| <a id="s-b93295a68c"></a>`description` | Optional nonnormative Windows mounted-volume reference for Gogurt. |
| <a id="s-9ce952e230"></a>`requires_python` | >=3.12 |
| <a id="s-4ab5c7b2c7"></a>`role` | reference_component |
| <a id="s-db1f11ebe7"></a>`source` | reference/gogurt/mounted-volume/windows/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-windows-mounted-volume](../../../evidence/relationships.md#rn-a05f279325)

## Governing policies

- <a id="pa-a460c54078"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-windows-mounted-volume](../../../evidence/sources.md#src-d3a2fa67cb) — `reference/gogurt/mounted-volume/windows/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-windows-mounted-volume`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a4ca08e11cac964598d90e405bdf9c4fd78d318d40f188b48e3733b3ce22c11 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_windows_mounted_volume-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_windows_mounted_volume-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Windows mounted-volume reference for Gogurt.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/mounted-volume/windows/pyproject.toml"
}
```
