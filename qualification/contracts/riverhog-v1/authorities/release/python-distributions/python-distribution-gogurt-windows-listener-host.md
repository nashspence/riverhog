# Python distribution: gogurt-windows-listener-host

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt-windows-listener-host:8275d61564 -->

Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ebd5f16ac3"></a>
| Concern | Contract |
|---|---|
| <a id="s-f433be29ee"></a>`artifacts` | `[{"coordinate":"dist/gogurt_windows_listener_host-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/gogurt_windows_listener_host-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-e01726257a"></a>`channel` | `"github-release"` |
| <a id="s-3403894e72"></a>`description` | `"Optional nonnormative Windows Task Scheduler listener-host reference for Gogurt."` |
| <a id="s-c1f74a1748"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-e1d21a023c"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-ffd4de11a2"></a>`publication_identity` | `{"coordinate":"gogurt-windows-listener-host","kind":"python-distribution"}` |
| <a id="s-be9156fe5b"></a>`requires_python` | `">=3.12"` |
| <a id="s-e371ec117a"></a>`role` | `"reference_component"` |
| <a id="s-1e001a5900"></a>`source` | `"reference/gogurt/listener-host/windows/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-windows-listener-host](../../../evidence/relationships/nodes.md#rn-4745107153)

## Governing policies

- <a id="pa-20de62dd5e"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-cf31971996"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:gogurt-windows-listener-host](../../../evidence/sources/authorities.md#src-99f0891144) — [reference/gogurt/listener-host/windows/pyproject.toml](../../../../../../reference/gogurt/listener-host/windows/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-windows-listener-host`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 31ac7dcba6f4ad57c66553075d679cfbd6c44974d7b89de4454171aa8672ede4 -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "gogurt-windows-listener-host",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/listener-host/windows/pyproject.toml"
}
```

</details>
