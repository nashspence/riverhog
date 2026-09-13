# Python distribution: stove0-observer-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-observer-client:e3b41cd3a6 -->

Narrow HTTP client for Stove0 content observers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-f5aa704990"></a>
| Concern | Contract |
|---|---|
| <a id="s-a65aed6d61"></a>`artifacts` | [{"coordinate": "dist/stove0_observer_client-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_observer_client-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-7e3e0e6263"></a>`channel` | github-release |
| <a id="s-4602d16bd8"></a>`description` | Narrow HTTP client for Stove0 content observers. |
| <a id="s-a3936a4f2e"></a>`requires_python` | >=3.12 |
| <a id="s-c28577a8a0"></a>`role` | reusable_library |
| <a id="s-3f311fd308"></a>`source` | reference/stove0/packages/observer-client/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-observer-client](../../../evidence/relationships.md#rn-21b6164c9f)

## Governing policies

- <a id="pa-265b7c7148"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-observer-client](../../../evidence/sources.md#src-ada50e7589) — `reference/stove0/packages/observer-client/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-observer-client`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9af9d91de5e95dffc13fdb16e19a2ee60691cfe6c9b603bc150602516676d4cf -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_observer_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_observer_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Narrow HTTP client for Stove0 content observers.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/observer-client/pyproject.toml"
}
```
