# Python distribution: review0-sampler-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-review0-sampler-client:8d82902208 -->

HTTP client for Review0 samplers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-f568e20cb6"></a>
| Concern | Contract |
|---|---|
| <a id="s-b369d64d45"></a>`artifacts` | `[{"coordinate":"dist/review0_sampler_client-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/review0_sampler_client-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-7f8f392a02"></a>`channel` | `"github-release"` |
| <a id="s-6ffd5cfe32"></a>`description` | `"HTTP client for Review0 samplers."` |
| <a id="s-ef9a8b04f7"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-bac064a792"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-c352e21bae"></a>`publication_identity` | `{"coordinate":"review0-sampler-client","kind":"python-distribution"}` |
| <a id="s-47bcb1ec86"></a>`requires_python` | `">=3.12"` |
| <a id="s-10e7613b88"></a>`role` | `"reusable_library"` |
| <a id="s-1028b385bb"></a>`source` | `"some-implementations/stove0/review0/sampler/client/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [review0-sampler-client](../../../evidence/relationships/nodes.md#rn-1082eec672)

## Governing policies

- <a id="pa-dc2c30e392"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-1a7813c31d"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:review0-sampler-client](../../../evidence/sources/authorities.md#src-b3ea00d0e6) — [some-implementations/stove0/review0/sampler/client/pyproject.toml](../../../../../../some-implementations/stove0/review0/sampler/client/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/review0-sampler-client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a249ac67b4bb12a0d38c43cdc7c9e1904cfbcbb3caf35a03b3003777d43db01f -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/review0_sampler_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/review0_sampler_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "HTTP client for Review0 samplers.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "review0-sampler-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/review0/sampler/client/pyproject.toml"
}
```

</details>
