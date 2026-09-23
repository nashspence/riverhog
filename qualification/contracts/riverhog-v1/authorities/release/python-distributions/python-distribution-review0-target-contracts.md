# Python distribution: review0-target-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-review0-target-contracts:4be1b88ee7 -->

Review0 target execution and artifact contracts.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-9e69d98ef5"></a>
| Concern | Contract |
|---|---|
| <a id="s-d7fb4c839d"></a>`artifacts` | `[{"coordinate":"dist/review0_target_contracts-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/review0_target_contracts-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-64e2105add"></a>`channel` | `"github-release"` |
| <a id="s-389d2652b8"></a>`description` | `"Review0 target execution and artifact contracts."` |
| <a id="s-11db067c81"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-4c2020c0af"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-f91c1c3700"></a>`publication_identity` | `{"coordinate":"review0-target-contracts","kind":"python-distribution"}` |
| <a id="s-981413985c"></a>`requires_python` | `">=3.12"` |
| <a id="s-577fda2a42"></a>`role` | `"reusable_library"` |
| <a id="s-4be97a8397"></a>`source` | `"some-implementations/stove0/review0/contracts/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [review0-target-contracts](../../../evidence/relationships/nodes.md#rn-73dc17d10c)

## Governing policies

- <a id="pa-a3563913de"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-3cf16845ff"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:review0-target-contracts](../../../evidence/sources/authorities.md#src-1dd6d6aaca) — [some-implementations/stove0/review0/contracts/pyproject.toml](../../../../../../some-implementations/stove0/review0/contracts/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/review0-target-contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97eacf81dcdff23a5b2988e6e1604c75e24d0f27e00d3978fff889033b70ecea -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/review0_target_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/review0_target_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Review0 target execution and artifact contracts.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "review0-target-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/review0/contracts/pyproject.toml"
}
```

</details>
