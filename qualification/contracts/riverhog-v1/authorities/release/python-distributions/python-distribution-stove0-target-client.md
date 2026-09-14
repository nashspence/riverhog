# Python distribution: stove0-target-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-target-client:8ebeeada4a -->

Narrow HTTP client for Stove0 transform targets.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-8942069ec1"></a>
| Concern | Contract |
|---|---|
| <a id="s-9e27a2563a"></a>`artifacts` | [{"coordinate": "dist/stove0_target_client-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_target_client-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-7fd919475a"></a>`channel` | github-release |
| <a id="s-8c88c6b4af"></a>`description` | Narrow HTTP client for Stove0 transform targets. |
| <a id="s-6d143f7f47"></a>`license_baseline` | first-v1-publication |
| <a id="s-0b75e39474"></a>`license_expression` | Apache-2.0 |
| <a id="s-5874aefd83"></a>`publication_identity` | {"coordinate": "stove0-target-client", "kind": "python-distribution"} |
| <a id="s-cb102729fa"></a>`requires_python` | >=3.12 |
| <a id="s-f66a0dc4a7"></a>`role` | reusable_library |
| <a id="s-b4c91f375a"></a>`source` | reference/stove0/packages/target-client/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-target-client](../../../evidence/relationships.md#rn-2c02e88004)

## Governing policies

- <a id="pa-8ae8bee064"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-9251dd3215"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-target-client](../../../evidence/sources.md#src-370da30421) — `reference/stove0/packages/target-client/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-target-client`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a075e44d4b49933da14ca1abf787bdf80104cca58eb1d1e52994753c9e351517 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_target_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_target_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Narrow HTTP client for Stove0 transform targets.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-target-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/target-client/pyproject.toml"
}
```
