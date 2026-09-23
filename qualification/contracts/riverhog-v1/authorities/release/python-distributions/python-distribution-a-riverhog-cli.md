# Python distribution: a-riverhog-cli

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-cli:8bc417f22d -->

Command-line client for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-7fb2f662e2"></a>
| Concern | Contract |
|---|---|
| <a id="s-8f144afc12"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_cli-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_cli-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-fbecc9d238"></a>`channel` | `"github-release"` |
| <a id="s-4399fcaca4"></a>`description` | `"Command-line client for Riverhog."` |
| <a id="s-c1085e2ba4"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-1a579ac900"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-ce56245edf"></a>`publication_identity` | `{"coordinate":"a-riverhog-cli","kind":"python-distribution"}` |
| <a id="s-44edb42f55"></a>`requires_python` | `">=3.12"` |
| <a id="s-55819cc869"></a>`role` | `"application"` |
| <a id="s-626b979d65"></a>`source` | `"some-implementations/riverhog/applications/a-riverhog-cli/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-cli](../../../evidence/relationships/nodes.md#rn-96e1191c92)

## Governing policies

- <a id="pa-3a0b971fd6"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-acd7b991b9"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d244665544) — [some-implementations/riverhog/applications/a-riverhog-cli/pyproject.toml](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-cli`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba8e9ef29a904b5169227def432a2afcb5a8a6e3999a2a72ff651324a84e05f1 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_cli-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_cli-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Command-line client for Riverhog.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-cli",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "application",
  "source": "some-implementations/riverhog/applications/a-riverhog-cli/pyproject.toml"
}
```

</details>
