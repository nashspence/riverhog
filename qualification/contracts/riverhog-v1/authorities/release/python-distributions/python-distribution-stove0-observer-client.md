# Python distribution: stove0-observer-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-observer-client:7f3a1a93a0 -->

Narrow HTTP client for Stove0 content observers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-f5aa704990"></a>
| Concern | Contract |
|---|---|
| <a id="s-a65aed6d61"></a>`artifacts` | `[{"coordinate":"dist/stove0_observer_client-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_observer_client-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-7e3e0e6263"></a>`channel` | `"github-release"` |
| <a id="s-4602d16bd8"></a>`description` | `"Narrow HTTP client for Stove0 content observers."` |
| <a id="s-42eb62ca35"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-5d259f470d"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-5727b46bb5"></a>`publication_identity` | `{"coordinate":"stove0-observer-client","kind":"python-distribution"}` |
| <a id="s-a3936a4f2e"></a>`requires_python` | `">=3.12"` |
| <a id="s-c28577a8a0"></a>`role` | `"reusable_library"` |
| <a id="s-3f311fd308"></a>`source` | `"some-implementations/stove0/packages/observer-client/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-observer-client](../../../evidence/relationships/nodes.md#rn-21b6164c9f)

## Governing policies

- <a id="pa-a4278f343f"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-7d3dd1dca5"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-observer-client](../../../evidence/sources/authorities.md#src-ada50e7589) — [some-implementations/stove0/packages/observer-client/pyproject.toml](../../../../../../some-implementations/stove0/packages/observer-client/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-observer-client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92d2619de7796f71d173481968bcbc336c4fbfe9bef73818e13c0601997c0446 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_observer_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_observer_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Narrow HTTP client for Stove0 content observers.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-observer-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/packages/observer-client/pyproject.toml"
}
```

</details>
