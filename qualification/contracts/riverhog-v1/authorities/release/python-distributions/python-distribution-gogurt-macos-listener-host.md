# Python distribution: gogurt-macos-listener-host

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt-macos-listener-host:246a0c20b8 -->

Optional nonnormative macOS launchd listener-host reference for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e6c28f237a"></a>
| Concern | Contract |
|---|---|
| <a id="s-e5c7287337"></a>`artifacts` | [{"coordinate": "dist/gogurt_macos_listener_host-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_macos_listener_host-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-11eb6b40d0"></a>`channel` | github-release |
| <a id="s-57438fbcf4"></a>`description` | Optional nonnormative macOS launchd listener-host reference for Gogurt. |
| <a id="s-933251baa1"></a>`license_baseline` | first-v1-publication |
| <a id="s-286cc8b885"></a>`license_expression` | Apache-2.0 |
| <a id="s-7dc3642ceb"></a>`publication_identity` | {"coordinate": "gogurt-macos-listener-host", "kind": "python-distribution"} |
| <a id="s-d999540960"></a>`requires_python` | >=3.12 |
| <a id="s-7ec8e97577"></a>`role` | reference_component |
| <a id="s-96901c782c"></a>`source` | reference/gogurt/listener-host/macos/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-macos-listener-host](../../../evidence/relationships.md#rn-a34362604e)

## Governing policies

- <a id="pa-3494ace5b1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-1d4441adbf"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-macos-listener-host](../../../evidence/sources.md#src-51ef474638) — `reference/gogurt/listener-host/macos/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-macos-listener-host`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a81dc6a2dcac63627628712ad0ab1a5c94ec389479b1239e4f836109e68e22cd -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_macos_listener_host-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_macos_listener_host-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative macOS launchd listener-host reference for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "gogurt-macos-listener-host",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/listener-host/macos/pyproject.toml"
}
```
