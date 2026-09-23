# Python distribution: a-review0-opus-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-review0-opus-sampler:12a51fd550 -->

Opus sampler for Review0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-58fbdec547"></a>
| Concern | Contract |
|---|---|
| <a id="s-1a0aec75dc"></a>`artifacts` | `[{"coordinate":"dist/a_review0_opus_sampler-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_review0_opus_sampler-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-743fbadef3"></a>`channel` | `"github-release"` |
| <a id="s-ece4c79db7"></a>`description` | `"Opus sampler for Review0."` |
| <a id="s-7992adfa67"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-d6b7a1a8f1"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-897eb953f8"></a>`publication_identity` | `{"coordinate":"a-review0-opus-sampler","kind":"python-distribution"}` |
| <a id="s-779940370c"></a>`requires_python` | `">=3.12"` |
| <a id="s-10148ec5f1"></a>`role` | `"component"` |
| <a id="s-a79c528278"></a>`source` | `"some-implementations/stove0/review0/samplers/opus/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-review0-opus-sampler](../../../evidence/relationships/nodes.md#rn-0d138eb003)

## Governing policies

- <a id="pa-87b3ecbdf5"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-b97c23c352"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-review0-opus-sampler](../../../evidence/sources/authorities.md#src-78f6847530) — [some-implementations/stove0/review0/samplers/opus/pyproject.toml](../../../../../../some-implementations/stove0/review0/samplers/opus/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-review0-opus-sampler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 939fbe837835703d97c0a14d68769b730132f494a26de98ae84104c43139b855 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_review0_opus_sampler-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_review0_opus_sampler-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Opus sampler for Review0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-review0-opus-sampler",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/review0/samplers/opus/pyproject.toml"
}
```

</details>
