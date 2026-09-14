# Python distribution: gogurt-linux-mounted-volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt-linux-mounted-volume:b608903957 -->

Optional nonnormative Linux mounted-volume reference for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-1cbe583a2e"></a>
| Concern | Contract |
|---|---|
| <a id="s-b3311ea87c"></a>`artifacts` | [{"coordinate": "dist/gogurt_linux_mounted_volume-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_linux_mounted_volume-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-ef3758d02e"></a>`channel` | github-release |
| <a id="s-59035f8d15"></a>`description` | Optional nonnormative Linux mounted-volume reference for Gogurt. |
| <a id="s-83a0763441"></a>`license_baseline` | first-v1-publication |
| <a id="s-901ec4b74c"></a>`license_expression` | Apache-2.0 |
| <a id="s-e0d396f579"></a>`publication_identity` | {"coordinate": "gogurt-linux-mounted-volume", "kind": "python-distribution"} |
| <a id="s-4ef7b003e5"></a>`requires_python` | >=3.12 |
| <a id="s-2fb1b3aa21"></a>`role` | reference_component |
| <a id="s-9c2ebd1b1a"></a>`source` | reference/gogurt/mounted-volume/linux/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-linux-mounted-volume](../../../evidence/relationships.md#rn-20b20b9fcd)

## Governing policies

- <a id="pa-26406273f5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-2a77c55e86"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-linux-mounted-volume](../../../evidence/sources.md#src-db58b362bd) — `reference/gogurt/mounted-volume/linux/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-linux-mounted-volume`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a613175379a776c4467f544629357991edb0c7ab3913fee344609768db0293e -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_linux_mounted_volume-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_linux_mounted_volume-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Linux mounted-volume reference for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "gogurt-linux-mounted-volume",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/mounted-volume/linux/pyproject.toml"
}
```
