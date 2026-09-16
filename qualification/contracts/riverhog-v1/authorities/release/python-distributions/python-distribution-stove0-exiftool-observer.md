# Python distribution: stove0-exiftool-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-exiftool-observer:6c00b31c61 -->

Optional nonnormative ExifTool observer reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-818871be67"></a>
| Concern | Contract |
|---|---|
| <a id="s-490c248e51"></a>`artifacts` | [{"coordinate": "dist/stove0_exiftool_observer-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_exiftool_observer-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-1a5d77a541"></a>`channel` | github-release |
| <a id="s-53ad5cc3ef"></a>`description` | Optional nonnormative ExifTool observer reference for Stove0. |
| <a id="s-7b63bed6ba"></a>`license_baseline` | first-v1-publication |
| <a id="s-3185b1215d"></a>`license_expression` | CAL-1.0 |
| <a id="s-3504cf307e"></a>`publication_identity` | {"coordinate": "stove0-exiftool-observer", "kind": "python-distribution"} |
| <a id="s-05f220f577"></a>`requires_python` | >=3.12 |
| <a id="s-b2656ca437"></a>`role` | reference_component |
| <a id="s-55ff83b6a8"></a>`source` | reference/stove0/observers/exiftool/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-exiftool-observer](../../../evidence/relationships.md#rn-9da0956020)

## Governing policies

- <a id="pa-2242563ea8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b1c3362a09"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-exiftool-observer](../../../evidence/sources.md#src-8b7b7e5eed) — `reference/stove0/observers/exiftool/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-exiftool-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa5c4e3135cf295974b0cde5ec4f887f16c5c59304d95bb6394461d81d901e3c -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_exiftool_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_exiftool_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative ExifTool observer reference for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "stove0-exiftool-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/observers/exiftool/pyproject.toml"
}
```

</details>
