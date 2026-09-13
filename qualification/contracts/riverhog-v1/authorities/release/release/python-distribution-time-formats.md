# Python distribution: time-formats

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-time-formats:bd18fc25fb -->

UTC timestamp and operator duration formats.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-5dfdfaae8f"></a>
| Concern | Contract |
|---|---|
| <a id="s-4db1498251"></a>`artifacts` | [{"coordinate": "dist/time_formats-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/time_formats-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-a4ec0f4512"></a>`channel` | github-release |
| <a id="s-367dc032fd"></a>`description` | UTC timestamp and operator duration formats. |
| <a id="s-1ba15713c9"></a>`requires_python` | >=3.12 |
| <a id="s-80ca4b9dcc"></a>`role` | internal_build_unit |
| <a id="s-c68c08fafe"></a>`source` | packages/time-formats/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [time-formats](../../../evidence/relationships.md#rn-71d1153198)

## Governing policies

- <a id="pa-e3c5242340"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:time-formats](../../../evidence/sources.md#src-514b283d5f) — `packages/time-formats/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/time-formats`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbd4f554e8ce4ce9f5ddc1c8bfdbd4b3a97b0b4c8d08818bab6bea15bb132f5b -->

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
  "requires_python": ">=3.12",
  "role": "internal_build_unit",
  "source": "packages/time-formats/pyproject.toml"
}
```
