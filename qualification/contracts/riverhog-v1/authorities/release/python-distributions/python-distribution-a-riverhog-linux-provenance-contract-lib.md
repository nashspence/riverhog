# Python distribution: a-riverhog-linux-provenance-contract-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-linux-prov-b71f940e56:a32999a44b -->

Linux filesystem observation contracts for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-120bcd66d7"></a>
| Concern | Contract |
|---|---|
| <a id="s-47361f4e11"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_linux_provenance_contract_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_linux_provenance_contract_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-dada34beab"></a>`channel` | `"github-release"` |
| <a id="s-8289ff15b2"></a>`description` | `"Linux filesystem observation contracts for Riverhog provenance."` |
| <a id="s-cb0cff058b"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-722ddeedb3"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-d73eefbe0b"></a>`publication_identity` | `{"coordinate":"a-riverhog-linux-provenance-contract-lib","kind":"python-distribution"}` |
| <a id="s-f1766644cd"></a>`requires_python` | `">=3.12"` |
| <a id="s-59fe8fc3a7"></a>`role` | `"component"` |
| <a id="s-3927e80de5"></a>`source` | `"some-implementations/riverhog/provenance/contracts/linux/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-linux-provenance-contract-lib](../../../evidence/relationships/nodes.md#rn-803b482d12)

## Governing policies

- <a id="pa-5c963c940d"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-e8821caba0"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-linux-provenance-contract-lib](../../../evidence/sources/authorities.md#src-38a8b52bb4) — [some-implementations/riverhog/provenance/contracts/linux/pyproject.toml](../../../../../../some-implementations/riverhog/provenance/contracts/linux/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-linux-provenance-contract-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae8963ed16c194e09cb004e55c94e16b0dff9c8486bf905c76ffc91b1165dab0 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_linux_provenance_contract_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_linux_provenance_contract_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Linux filesystem observation contracts for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-linux-provenance-contract-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/provenance/contracts/linux/pyproject.toml"
}
```

</details>
