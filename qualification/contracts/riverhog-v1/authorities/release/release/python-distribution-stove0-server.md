# Python distribution: stove0-server

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-server:d469eca52a -->

Optional nonnormative content-opaque transformation reference application for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-5514b94010"></a>
| Concern | Contract |
|---|---|
| <a id="s-9651bdc910"></a>`artifacts` | [{"coordinate": "dist/stove0_server-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_server-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-e23276f413"></a>`channel` | github-release |
| <a id="s-1ac4e1d764"></a>`description` | Optional nonnormative content-opaque transformation reference application for Riverhog. |
| <a id="s-c6b51d86a4"></a>`requires_python` | >=3.12 |
| <a id="s-a8e05f575a"></a>`role` | reference_application |
| <a id="s-79dee626a1"></a>`source` | reference/stove0/application/server/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-server](../../../evidence/relationships.md#rn-3540de4d4a)

## Governing policies

- <a id="pa-c2a3369b90"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-server](../../../evidence/sources.md#src-56a02fc153) — `reference/stove0/application/server/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-server`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a212b2a1e22177ffc5bda4238a9483b4f84e279ad672839e2caaed5f30a34932 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_server-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_server-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative content-opaque transformation reference application for Riverhog.",
  "requires_python": ">=3.12",
  "role": "reference_application",
  "source": "reference/stove0/application/server/pyproject.toml"
}
```
