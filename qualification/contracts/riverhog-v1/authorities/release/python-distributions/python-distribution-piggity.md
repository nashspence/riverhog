# Python distribution: piggity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-piggity:d72e3de851 -->

Optional nonnormative Piggity reference client for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-1966fb3cb2"></a>
| Concern | Contract |
|---|---|
| <a id="s-5d7e2b6578"></a>`artifacts` | [{"coordinate": "dist/piggity-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/piggity-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-636a1abf06"></a>`channel` | github-release |
| <a id="s-038ee865b8"></a>`description` | Optional nonnormative Piggity reference client for Riverhog. |
| <a id="s-3c99dd37ba"></a>`license_baseline` | first-v1-publication |
| <a id="s-b663621de1"></a>`license_expression` | Apache-2.0 |
| <a id="s-36f6316d1a"></a>`publication_identity` | {"coordinate": "piggity", "kind": "python-distribution"} |
| <a id="s-fa961fb715"></a>`requires_python` | >=3.12 |
| <a id="s-c0be743c48"></a>`role` | reference_application |
| <a id="s-b5bd5257fc"></a>`source` | reference/riverhog/applications/piggity/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [piggity](../../../evidence/relationships.md#rn-0a279524f4)

## Governing policies

- <a id="pa-6e52726d3d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-74ee9ea705"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:piggity](../../../evidence/sources.md#src-c6f8868ecf) — `reference/riverhog/applications/piggity/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/piggity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8cf571e07f19991c506c0fcfc4d7c65a13173a9d40975a5dea2bdcabd822192e -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/piggity-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/piggity-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Piggity reference client for Riverhog.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "piggity",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_application",
  "source": "reference/riverhog/applications/piggity/pyproject.toml"
}
```
