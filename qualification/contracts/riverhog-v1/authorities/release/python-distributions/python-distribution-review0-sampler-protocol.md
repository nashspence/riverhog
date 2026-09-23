# Python distribution: review0-sampler-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-review0-sampler-protocol:440194e77a -->

Protocol contracts for Review0 samplers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e41eeaaf68"></a>
| Concern | Contract |
|---|---|
| <a id="s-4077709fb5"></a>`artifacts` | `[{"coordinate":"dist/review0_sampler_protocol-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/review0_sampler_protocol-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-5948680de5"></a>`channel` | `"github-release"` |
| <a id="s-1d04143a91"></a>`description` | `"Protocol contracts for Review0 samplers."` |
| <a id="s-82b638251c"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-7c3d4956eb"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-6dbab6081b"></a>`publication_identity` | `{"coordinate":"review0-sampler-protocol","kind":"python-distribution"}` |
| <a id="s-ea408c4225"></a>`requires_python` | `">=3.12"` |
| <a id="s-f263f0b985"></a>`role` | `"reusable_library"` |
| <a id="s-e8a55b2664"></a>`source` | `"some-implementations/stove0/review0/sampler/protocol/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [review0-sampler-protocol](../../../evidence/relationships/nodes.md#rn-e32e317bb5)

## Governing policies

- <a id="pa-0ac5826fd3"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-2050a8958c"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:review0-sampler-protocol](../../../evidence/sources/authorities.md#src-3533c260e3) — [some-implementations/stove0/review0/sampler/protocol/pyproject.toml](../../../../../../some-implementations/stove0/review0/sampler/protocol/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/review0-sampler-protocol`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 69cc3346fa26f7a256bc66093da733c7c1168fde236ffc856b92e3ec9bc03c10 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/review0_sampler_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/review0_sampler_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Protocol contracts for Review0 samplers.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "review0-sampler-protocol",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/review0/sampler/protocol/pyproject.toml"
}
```

</details>
