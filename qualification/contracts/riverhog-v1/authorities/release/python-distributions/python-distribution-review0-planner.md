# Python distribution: review0-planner

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-review0-planner:86870789d8 -->

Review0 plan construction for Stove0 workflows.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-01eaceb82b"></a>
| Concern | Contract |
|---|---|
| <a id="s-f359cd3945"></a>`artifacts` | `[{"coordinate":"dist/review0_planner-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/review0_planner-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-b273dae75d"></a>`channel` | `"github-release"` |
| <a id="s-9a212eb3cd"></a>`description` | `"Review0 plan construction for Stove0 workflows."` |
| <a id="s-f4f0ac203b"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-9cea44076c"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-2629aa43bd"></a>`publication_identity` | `{"coordinate":"review0-planner","kind":"python-distribution"}` |
| <a id="s-f27b72767a"></a>`requires_python` | `">=3.12"` |
| <a id="s-8fcdda197e"></a>`role` | `"component"` |
| <a id="s-8cd6087a6b"></a>`source` | `"some-implementations/stove0/review0/planning/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [review0-planner](../../../evidence/relationships/nodes.md#rn-5324da9af2)

## Governing policies

- <a id="pa-7c3566ac80"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-b183239cbd"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:review0-planner](../../../evidence/sources/authorities.md#src-9548af5561) — [some-implementations/stove0/review0/planning/pyproject.toml](../../../../../../some-implementations/stove0/review0/planning/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/review0-planner`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32fa301378d9f64729535af2ae7c0492bbbce441cc2f08a5e12197b6c556015a -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/review0_planner-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/review0_planner-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Review0 plan construction for Stove0 workflows.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "review0-planner",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/review0/planning/pyproject.toml"
}
```

</details>
