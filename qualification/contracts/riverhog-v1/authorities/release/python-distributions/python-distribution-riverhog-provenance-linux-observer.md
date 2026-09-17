# Python distribution: riverhog-provenance-linux-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-provenance-l-e2232b8bf8:79a6c03e63 -->

Optional nonnormative Linux filesystem-observer reference for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e7d5bd199d"></a>
| Concern | Contract |
|---|---|
| <a id="s-1008eaa767"></a>`artifacts` | `[{"coordinate":"dist/riverhog_provenance_linux_observer-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_provenance_linux_observer-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-65eaa7ffc6"></a>`channel` | `"github-release"` |
| <a id="s-04e1732c93"></a>`description` | `"Optional nonnormative Linux filesystem-observer reference for Riverhog provenance."` |
| <a id="s-34e9032db8"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-0d82110ad8"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-43fdd3af24"></a>`publication_identity` | `{"coordinate":"riverhog-provenance-linux-observer","kind":"python-distribution"}` |
| <a id="s-bdb62355be"></a>`requires_python` | `">=3.12"` |
| <a id="s-fde1490df6"></a>`role` | `"reference_component"` |
| <a id="s-7e8cc31437"></a>`source` | `"reference/riverhog/provenance/observers/linux/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance-linux-observer](../../../evidence/relationships/nodes.md#rn-f733d7aee6)

## Governing policies

- <a id="pa-e5580b9015"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-dad2e582a4"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-provenance-linux-observer](../../../evidence/sources/authorities.md#src-511bdc989a) — [reference/riverhog/provenance/observers/linux/pyproject.toml](../../../../../../reference/riverhog/provenance/observers/linux/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance-linux-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd9ee6ee6e16cbfa6ecb57b0d0843824e5dd992e41229681ca0d45d023faf24e -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance_linux_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance_linux_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Linux filesystem-observer reference for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-provenance-linux-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/provenance/observers/linux/pyproject.toml"
}
```

</details>
