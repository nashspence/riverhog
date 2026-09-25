# Python distribution: a-riverhog-opentimestamps-witness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-opentimest-f3684564c9:7056485a0b -->

Independent OpenTimestamps collection witness for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-33a40af3c5"></a>
| Concern | Contract |
|---|---|
| <a id="s-bb33314ca3"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_opentimestamps_witness-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_opentimestamps_witness-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-6052077b85"></a>`channel` | `"github-release"` |
| <a id="s-8c978ce49a"></a>`description` | `"Independent OpenTimestamps collection witness for Riverhog."` |
| <a id="s-a2b663f2cd"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-13b3af16e8"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-89ced92eb6"></a>`publication_identity` | `{"coordinate":"a-riverhog-opentimestamps-witness","kind":"python-distribution"}` |
| <a id="s-01ec258e94"></a>`requires_python` | `">=3.12"` |
| <a id="s-8876f52194"></a>`role` | `"application"` |
| <a id="s-25da93a8f8"></a>`source` | `"some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-opentimestamps-witness](../../../evidence/relationships/nodes.md#rn-44dc2bd3f2)

## Governing policies

- <a id="pa-b7b0d2572a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-68f8af384e"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-f5e7540879) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/pyproject.toml](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-opentimestamps-witness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48dda12c272884bfa8c21af5979962d11969d174a4ad476546a42e6411d5af6f -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_opentimestamps_witness-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_opentimestamps_witness-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Independent OpenTimestamps collection witness for Riverhog.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-opentimestamps-witness",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "application",
  "source": "some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/pyproject.toml"
}
```

</details>
