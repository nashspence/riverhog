# Python distribution: time-formats

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-time-formats:ffe5f36d5c -->

UTC timestamp and operator duration formats.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-5dfdfaae8f"></a>
| Concern | Contract |
|---|---|
| <a id="s-4db1498251"></a>`artifacts` | `[{"coordinate":"dist/time_formats-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/time_formats-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-a4ec0f4512"></a>`channel` | `"github-release"` |
| <a id="s-367dc032fd"></a>`description` | `"UTC timestamp and operator duration formats."` |
| <a id="s-ad31416e60"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-c619177636"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-91fa562265"></a>`publication_identity` | `{"coordinate":"time-formats","kind":"python-distribution"}` |
| <a id="s-1ba15713c9"></a>`requires_python` | `">=3.12"` |
| <a id="s-80ca4b9dcc"></a>`role` | `"internal_build_unit"` |
| <a id="s-c68c08fafe"></a>`source` | `"packages/time-formats/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [time-formats](../../../evidence/relationships/nodes.md#rn-71d1153198)

## Governing policies

- <a id="pa-9a20976b66"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-091db6fe1e"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:time-formats](../../../evidence/sources/authorities.md#src-514b283d5f) — [packages/time-formats/pyproject.toml](../../../../../../packages/time-formats/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/time-formats`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54ecfaa95bddcdf6888738345c304b629c197e6301e2b8f70313c76630ff180a -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/time_formats-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/time_formats-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "UTC timestamp and operator duration formats.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "time-formats",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "internal_build_unit",
  "source": "packages/time-formats/pyproject.toml"
}
```

</details>
