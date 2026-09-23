# Python distribution: a-stove0-exiftool-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-exiftool-observer:7ca6b022c8 -->

ExifTool media metadata observer for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-0020c706d5"></a>
| Concern | Contract |
|---|---|
| <a id="s-542625fd94"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_exiftool_observer-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_exiftool_observer-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-51b090281a"></a>`channel` | `"github-release"` |
| <a id="s-07e480bebf"></a>`description` | `"ExifTool media metadata observer for Stove0."` |
| <a id="s-0b9e3e8bc9"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-c763e28353"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-a3b488cdc3"></a>`publication_identity` | `{"coordinate":"a-stove0-exiftool-observer","kind":"python-distribution"}` |
| <a id="s-ede1e608cc"></a>`requires_python` | `">=3.12"` |
| <a id="s-79422ce992"></a>`role` | `"component"` |
| <a id="s-d6ce516a02"></a>`source` | `"some-implementations/stove0/observers/exiftool/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-exiftool-observer](../../../evidence/relationships/nodes.md#rn-5dd092712c)

## Governing policies

- <a id="pa-6286456e7f"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-6628810805"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-exiftool-observer](../../../evidence/sources/authorities.md#src-d9b1da0f33) — [some-implementations/stove0/observers/exiftool/pyproject.toml](../../../../../../some-implementations/stove0/observers/exiftool/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-exiftool-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c8a8aaa663d1a75569ff3c9e5f1474a4390985d5a7d7ccf7d2e1d583b1d65141 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_exiftool_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_exiftool_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "ExifTool media metadata observer for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-stove0-exiftool-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/observers/exiftool/pyproject.toml"
}
```

</details>
