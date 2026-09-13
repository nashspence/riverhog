# Python distribution: mango-fish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-mango-fish:e172aa8079 -->

Optional nonnormative CloudEvents reference application for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e3a22b60f2"></a>
| Concern | Contract |
|---|---|
| <a id="s-b82f4b2ef0"></a>`artifacts` | [{"coordinate": "dist/mango_fish-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/mango_fish-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-b1ae085e1c"></a>`channel` | github-release |
| <a id="s-53714ce8a6"></a>`description` | Optional nonnormative CloudEvents reference application for Riverhog. |
| <a id="s-d09208ca8a"></a>`requires_python` | >=3.12 |
| <a id="s-963c86a994"></a>`role` | reference_application |
| <a id="s-f10f589664"></a>`source` | reference/riverhog/applications/mango-fish/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [mango-fish](../../../evidence/relationships.md#rn-1c3900c994)

## Governing policies

- <a id="pa-2df23a890d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-24fec5749f"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:mango-fish](../../../evidence/sources.md#src-8f9c704431) — `reference/riverhog/applications/mango-fish/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/mango-fish`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19bd3f90f82f74a2c740733814bde6f3f5017d8fa0213e900d595b6a9d728b87 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/mango_fish-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/mango_fish-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative CloudEvents reference application for Riverhog.",
  "requires_python": ">=3.12",
  "role": "reference_application",
  "source": "reference/riverhog/applications/mango-fish/pyproject.toml"
}
```
