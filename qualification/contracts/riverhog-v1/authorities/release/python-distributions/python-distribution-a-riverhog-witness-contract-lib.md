# Python distribution: a-riverhog-witness-contract-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-witness-contract-lib:1714b02089 -->

Shared collection witness statement for supplied Riverhog witness applications.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e66dcbd603"></a>
| Concern | Contract |
|---|---|
| <a id="s-79a506d503"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_witness_contract_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_witness_contract_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-f9a28d34ac"></a>`channel` | `"github-release"` |
| <a id="s-d346240bec"></a>`description` | `"Shared collection witness statement for supplied Riverhog witness applications."` |
| <a id="s-3230904e7f"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-d2480cee20"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-5112bdeac9"></a>`publication_identity` | `{"coordinate":"a-riverhog-witness-contract-lib","kind":"python-distribution"}` |
| <a id="s-f12a6834cc"></a>`requires_python` | `">=3.12"` |
| <a id="s-fc86f1059e"></a>`role` | `"reusable_library"` |
| <a id="s-ec351ad5b9"></a>`source` | `"some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-witness-contract-lib](../../../evidence/relationships/nodes.md#rn-2af264f778)

## Governing policies

- <a id="pa-c6b856fe2f"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-6d1bff9abb"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-witness-contract-lib](../../../evidence/sources/authorities.md#src-eba72b6f72) — [some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/pyproject.toml](../../../../../../some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-witness-contract-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee2a1a2883a7c22b0cb87735d7c42c07dfe501adf9139ee5dd353c988970e12d -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_witness_contract_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_witness_contract_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Shared collection witness statement for supplied Riverhog witness applications.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-witness-contract-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/pyproject.toml"
}
```

</details>
