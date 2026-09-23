# Python distribution: review0-sampler-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-review0-sampler-lib:1203852145 -->

Runtime and conformance support for Review0 samplers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-9611643192"></a>
| Concern | Contract |
|---|---|
| <a id="s-549e2b3a5f"></a>`artifacts` | `[{"coordinate":"dist/review0_sampler_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/review0_sampler_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-2e200a80f9"></a>`channel` | `"github-release"` |
| <a id="s-13b21a57f7"></a>`description` | `"Runtime and conformance support for Review0 samplers."` |
| <a id="s-6f819e4c95"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-0f6dde1030"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-f59a1bbcb6"></a>`publication_identity` | `{"coordinate":"review0-sampler-lib","kind":"python-distribution"}` |
| <a id="s-12b9555dc6"></a>`requires_python` | `">=3.12"` |
| <a id="s-6304c0fd63"></a>`role` | `"reusable_library"` |
| <a id="s-1fceecb605"></a>`source` | `"some-implementations/stove0/review0/sampler/support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [review0-sampler-lib](../../../evidence/relationships/nodes.md#rn-1f47183561)

## Governing policies

- <a id="pa-a277d0199a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-f4aae8e2a9"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:review0-sampler-lib](../../../evidence/sources/authorities.md#src-6e0c12e1b2) — [some-implementations/stove0/review0/sampler/support/pyproject.toml](../../../../../../some-implementations/stove0/review0/sampler/support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/review0-sampler-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b17e3258e2c114bd768a1287ec5a0537fa8c59ad0d008d8c776ae384568c387 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/review0_sampler_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/review0_sampler_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Runtime and conformance support for Review0 samplers.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "review0-sampler-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/review0/sampler/support/pyproject.toml"
}
```

</details>
