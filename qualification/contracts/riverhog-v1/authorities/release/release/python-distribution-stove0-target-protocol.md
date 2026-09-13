# Python distribution: stove0-target-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-target-protocol:bac086e0b2 -->

Dependency-light public contracts for external stove0 targets.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-b28b2b34b2"></a>
| Concern | Contract |
|---|---|
| <a id="s-764e625f32"></a>`artifacts` | [{"coordinate": "dist/stove0_target_protocol-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_target_protocol-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-125473a001"></a>`channel` | github-release |
| <a id="s-67d653caf2"></a>`description` | Dependency-light public contracts for external stove0 targets. |
| <a id="s-bf7c3b499d"></a>`requires_python` | >=3.12 |
| <a id="s-413d844e13"></a>`role` | reusable_library |
| <a id="s-2a2b42bd50"></a>`source` | reference/stove0/packages/target-protocol/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-target-protocol](../../../evidence/relationships.md#rn-2bfbd86b00)

## Governing policies

- <a id="pa-5b67cd6461"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-target-protocol](../../../evidence/sources.md#src-182457b760) — `reference/stove0/packages/target-protocol/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-target-protocol`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ca38c4549f629d797f3d7c1ab7780249162c9cf240302b2e43c69b3e564beb8 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_target_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_target_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Dependency-light public contracts for external stove0 targets.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/target-protocol/pyproject.toml"
}
```
