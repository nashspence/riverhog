# Python distribution: a-riverhog-windows-provenance-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-windows-pr-b1ba61400e:65693af614 -->

Windows filesystem observer for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-6ca1dc0bf1"></a>
| Concern | Contract |
|---|---|
| <a id="s-209e5cec42"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_windows_provenance_observer-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_windows_provenance_observer-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-b9e152ccba"></a>`channel` | `"github-release"` |
| <a id="s-ca152f310f"></a>`description` | `"Windows filesystem observer for Riverhog provenance."` |
| <a id="s-2061cc449b"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-b91a5b198f"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-e551ac40e1"></a>`publication_identity` | `{"coordinate":"a-riverhog-windows-provenance-observer","kind":"python-distribution"}` |
| <a id="s-9bfe64abe1"></a>`requires_python` | `">=3.12"` |
| <a id="s-f707762265"></a>`role` | `"component"` |
| <a id="s-7630b7532d"></a>`source` | `"some-implementations/riverhog/provenance/observers/windows/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-windows-provenance-observer](../../../evidence/relationships/nodes.md#rn-a6eefbe109)

## Governing policies

- <a id="pa-844843b28a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-47600e8f5d"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-windows-provenance-observer](../../../evidence/sources/authorities.md#src-cacdf9e80d) — [some-implementations/riverhog/provenance/observers/windows/pyproject.toml](../../../../../../some-implementations/riverhog/provenance/observers/windows/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-windows-provenance-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b99fa1de032b794c7c37509fc6a01562a415394d829155ac78d7db8284c3185d -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_windows_provenance_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_windows_provenance_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Windows filesystem observer for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-windows-provenance-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/provenance/observers/windows/pyproject.toml"
}
```

</details>
