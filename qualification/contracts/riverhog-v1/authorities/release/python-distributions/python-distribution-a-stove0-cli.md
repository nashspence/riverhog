# Python distribution: a-stove0-cli

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-cli:bda92261e7 -->

Command-line client for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-8de1b56e2d"></a>
| Concern | Contract |
|---|---|
| <a id="s-d63f631df5"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_cli-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_cli-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-1080243b51"></a>`channel` | `"github-release"` |
| <a id="s-c3578b43b0"></a>`description` | `"Command-line client for Stove0."` |
| <a id="s-7402905467"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-df21acfbd3"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-efbc4eae1f"></a>`publication_identity` | `{"coordinate":"a-stove0-cli","kind":"python-distribution"}` |
| <a id="s-479ac7aadd"></a>`requires_python` | `">=3.12"` |
| <a id="s-a38327e0f1"></a>`role` | `"application"` |
| <a id="s-963279055d"></a>`source` | `"some-implementations/stove0/application/client/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-cli](../../../evidence/relationships/nodes.md#rn-acec7cb547)

## Governing policies

- <a id="pa-bc1ad7d856"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-2b5a7abb20"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-cli](../../../evidence/sources/authorities.md#src-1a670e9d1d) — [some-implementations/stove0/application/client/pyproject.toml](../../../../../../some-implementations/stove0/application/client/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-cli`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89da7391d5bb09aaceb0b5b89c86fa1b8c1de828c9908be2994a3bf11b9613fa -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_cli-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_cli-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Command-line client for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-stove0-cli",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "application",
  "source": "some-implementations/stove0/application/client/pyproject.toml"
}
```

</details>
