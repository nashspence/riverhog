# Python distribution: a-review0-materializer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-review0-materializer:e7bc0140d0 -->

Review0 materialization target for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-07d416066d"></a>
| Concern | Contract |
|---|---|
| <a id="s-6c9306e49f"></a>`artifacts` | `[{"coordinate":"dist/a_review0_materializer-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_review0_materializer-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-6538c139ac"></a>`channel` | `"github-release"` |
| <a id="s-03371e8538"></a>`description` | `"Review0 materialization target for Stove0."` |
| <a id="s-202229520a"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-bbc680bc5b"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-a9876560f6"></a>`publication_identity` | `{"coordinate":"a-review0-materializer","kind":"python-distribution"}` |
| <a id="s-eb2c0f2837"></a>`requires_python` | `">=3.12"` |
| <a id="s-1d8e6094f1"></a>`role` | `"component"` |
| <a id="s-a40d587a5c"></a>`source` | `"some-implementations/stove0/review0/materialize-target/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-review0-materializer](../../../evidence/relationships/nodes.md#rn-df33691267)

## Governing policies

- <a id="pa-31c3ade26c"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-0ba6db8289"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-review0-materializer](../../../evidence/sources/authorities.md#src-6fe79170b1) — [some-implementations/stove0/review0/materialize-target/pyproject.toml](../../../../../../some-implementations/stove0/review0/materialize-target/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-review0-materializer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00a4a9e47107119751126b7a8dd0d85431d75c7320d9d8f735f02daf2f96cc60 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_review0_materializer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_review0_materializer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Review0 materialization target for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-review0-materializer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/review0/materialize-target/pyproject.toml"
}
```

</details>
