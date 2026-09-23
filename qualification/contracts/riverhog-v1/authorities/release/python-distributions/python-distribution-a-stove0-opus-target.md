# Python distribution: a-stove0-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-opus-target:15a6e1a0c4 -->

Opus transformation target for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-37e2ac2a0d"></a>
| Concern | Contract |
|---|---|
| <a id="s-9def961c80"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_opus_target-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_opus_target-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-f222601454"></a>`channel` | `"github-release"` |
| <a id="s-60c9458452"></a>`description` | `"Opus transformation target for Stove0."` |
| <a id="s-cb506a95a3"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-99049d180c"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-c3bd7c4321"></a>`publication_identity` | `{"coordinate":"a-stove0-opus-target","kind":"python-distribution"}` |
| <a id="s-4cf802d95a"></a>`requires_python` | `">=3.12"` |
| <a id="s-6e7dbcee74"></a>`role` | `"component"` |
| <a id="s-8ab77b3c0b"></a>`source` | `"some-implementations/stove0/targets/opus/target/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-opus-target](../../../evidence/relationships/nodes.md#rn-9ac2ba7d16)

## Governing policies

- <a id="pa-0d543de77e"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-776d7d672e"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-opus-target](../../../evidence/sources/authorities.md#src-d22e79391d) — [some-implementations/stove0/targets/opus/target/pyproject.toml](../../../../../../some-implementations/stove0/targets/opus/target/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-opus-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d5baca38494f153212af02569730cef76bcff2ea7fec58d7778c048cd04c7e5 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_opus_target-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_opus_target-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Opus transformation target for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-stove0-opus-target",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/targets/opus/target/pyproject.toml"
}
```

</details>
