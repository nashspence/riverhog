# Python distribution: a-riverhog-macos-provenance-contract-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-macos-prov-23e875c757:e5c9d12ef8 -->

macOS filesystem observation contracts for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-178ae17b3f"></a>
| Concern | Contract |
|---|---|
| <a id="s-38961bb3a2"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_macos_provenance_contract_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_macos_provenance_contract_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-83000edea6"></a>`channel` | `"github-release"` |
| <a id="s-59128e6348"></a>`description` | `"macOS filesystem observation contracts for Riverhog provenance."` |
| <a id="s-7f0b405e09"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-013f000404"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-0d219f4315"></a>`publication_identity` | `{"coordinate":"a-riverhog-macos-provenance-contract-lib","kind":"python-distribution"}` |
| <a id="s-ff9baa968b"></a>`requires_python` | `">=3.12"` |
| <a id="s-3dc5926533"></a>`role` | `"component"` |
| <a id="s-fe98516f12"></a>`source` | `"some-implementations/riverhog/provenance/contracts/macos/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-macos-provenance-contract-lib](../../../evidence/relationships/nodes.md#rn-42f1a175c2)

## Governing policies

- <a id="pa-32c668dba1"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-9abcaf2d5e"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-macos-provenance-contract-lib](../../../evidence/sources/authorities.md#src-b5c1a90eef) — [some-implementations/riverhog/provenance/contracts/macos/pyproject.toml](../../../../../../some-implementations/riverhog/provenance/contracts/macos/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-macos-provenance-contract-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b706139d18d222eebb1e98ba571e41d8e2af7812ce3e3f52eab14f306a8cd51a -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_macos_provenance_contract_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_macos_provenance_contract_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "macOS filesystem observation contracts for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-macos-provenance-contract-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/provenance/contracts/macos/pyproject.toml"
}
```

</details>
