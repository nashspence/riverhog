# Python distribution: a-riverhog-linux-provenance-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-linux-prov-c9b1fe9dd1:65b732fde2 -->

Linux filesystem observer for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-efecd9c7fc"></a>
| Concern | Contract |
|---|---|
| <a id="s-f92a83e8da"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_linux_provenance_observer-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_linux_provenance_observer-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-3c66e6d310"></a>`channel` | `"github-release"` |
| <a id="s-fba483e8ed"></a>`description` | `"Linux filesystem observer for Riverhog provenance."` |
| <a id="s-78af62bf38"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-f6aabda46c"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-96fa220e53"></a>`publication_identity` | `{"coordinate":"a-riverhog-linux-provenance-observer","kind":"python-distribution"}` |
| <a id="s-daea31c6a4"></a>`requires_python` | `">=3.12"` |
| <a id="s-ce43342f24"></a>`role` | `"component"` |
| <a id="s-b31e5f1fbe"></a>`source` | `"some-implementations/riverhog/provenance/observers/linux/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-linux-provenance-observer](../../../evidence/relationships/nodes.md#rn-84baf121e9)

## Governing policies

- <a id="pa-aced699562"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-1f6d6672f0"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-linux-provenance-observer](../../../evidence/sources/authorities.md#src-04e9b1a271) — [some-implementations/riverhog/provenance/observers/linux/pyproject.toml](../../../../../../some-implementations/riverhog/provenance/observers/linux/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-linux-provenance-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 005f87f711a67fefc60f15ce5e99755f915457bb86cd1433da37f90f8f35806b -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_linux_provenance_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_linux_provenance_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Linux filesystem observer for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-linux-provenance-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/provenance/observers/linux/pyproject.toml"
}
```

</details>
