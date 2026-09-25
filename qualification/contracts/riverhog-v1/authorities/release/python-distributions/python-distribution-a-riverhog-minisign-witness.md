# Python distribution: a-riverhog-minisign-witness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-minisign-witness:5aa16694c7 -->

Independent Minisign collection witness for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-c7ef9d004b"></a>
| Concern | Contract |
|---|---|
| <a id="s-0edc204953"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_minisign_witness-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_minisign_witness-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-743f379c06"></a>`channel` | `"github-release"` |
| <a id="s-fb63c42381"></a>`description` | `"Independent Minisign collection witness for Riverhog."` |
| <a id="s-e0a0230d75"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-4ce41aca59"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-f104416b0d"></a>`publication_identity` | `{"coordinate":"a-riverhog-minisign-witness","kind":"python-distribution"}` |
| <a id="s-128c5d1e2f"></a>`requires_python` | `">=3.12"` |
| <a id="s-32c2b13fdb"></a>`role` | `"application"` |
| <a id="s-47b0ef1e24"></a>`source` | `"some-implementations/riverhog/applications/a-riverhog-minisign-witness/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-minisign-witness](../../../evidence/relationships/nodes.md#rn-169780f44a)

## Governing policies

- <a id="pa-56a78347b4"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-2ae61a5e24"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-44a02dc84b) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/pyproject.toml](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-minisign-witness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7fa19757d8c8cf1fc253c00710dc615f5f0e8564407487c65601a5537098377 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_minisign_witness-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_minisign_witness-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Independent Minisign collection witness for Riverhog.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-minisign-witness",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "application",
  "source": "some-implementations/riverhog/applications/a-riverhog-minisign-witness/pyproject.toml"
}
```

</details>
