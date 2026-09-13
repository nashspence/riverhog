# Python distribution: gogurt-windows-listener-host

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-gogurt-windows-listener-host:e61f4a9ce2 -->

Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-ebd5f16ac3"></a>
| Concern | Contract |
|---|---|
| <a id="s-f433be29ee"></a>`artifacts` | [{"coordinate": "dist/gogurt_windows_listener_host-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_windows_listener_host-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-e01726257a"></a>`channel` | github-release |
| <a id="s-3403894e72"></a>`description` | Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt. |
| <a id="s-be9156fe5b"></a>`requires_python` | >=3.12 |
| <a id="s-e371ec117a"></a>`role` | reference_component |
| <a id="s-1e001a5900"></a>`source` | reference/gogurt/listener-host/windows/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-windows-listener-host](../../../evidence/relationships.md#rn-4745107153)

## Governing policies

- <a id="pa-75e59ab2f7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-windows-listener-host](../../../evidence/sources.md#src-99f0891144) — `reference/gogurt/listener-host/windows/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-windows-listener-host`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2160ea0755e5cc73083cfa715a7a7b0386d323f49eefffcb6b25f5816c0b2da8 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_windows_listener_host-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_windows_listener_host-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/listener-host/windows/pyproject.toml"
}
```
