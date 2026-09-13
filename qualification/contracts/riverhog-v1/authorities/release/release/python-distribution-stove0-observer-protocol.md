# Python distribution: stove0-observer-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-observer-protocol:dd43c16d3f -->

Dependency-light public contracts for external stove0 content observers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-1fb2121fd2"></a>
| Concern | Contract |
|---|---|
| <a id="s-0367d3bc37"></a>`artifacts` | [{"coordinate": "dist/stove0_observer_protocol-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_observer_protocol-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-0e6acfa8b5"></a>`channel` | github-release |
| <a id="s-430672fc88"></a>`description` | Dependency-light public contracts for external stove0 content observers. |
| <a id="s-26e66e614c"></a>`requires_python` | >=3.12 |
| <a id="s-3a08d0f376"></a>`role` | reusable_library |
| <a id="s-a8185be19d"></a>`source` | reference/stove0/packages/observer-protocol/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-observer-protocol](../../../evidence/relationships.md#rn-bac58a079c)

## Governing policies

- <a id="pa-345eed4c9a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-observer-protocol](../../../evidence/sources.md#src-7c927fed76) — `reference/stove0/packages/observer-protocol/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-observer-protocol`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e38b4a18dd85dd22d5409984be01d02a13efd300c816082e0c06663d4774a6f5 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_observer_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_observer_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Dependency-light public contracts for external stove0 content observers.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/observer-protocol/pyproject.toml"
}
```
