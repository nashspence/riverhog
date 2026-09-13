# Python distribution: stove0-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-protocol:424184f317 -->

Canonical content-opaque collection orchestration contracts for stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-21ab1d34d4"></a>
| Concern | Contract |
|---|---|
| <a id="s-a1b4ed2273"></a>`artifacts` | [{"coordinate": "dist/stove0_protocol-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_protocol-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-93af748a18"></a>`channel` | github-release |
| <a id="s-a3cca36246"></a>`description` | Canonical content-opaque collection orchestration contracts for stove0. |
| <a id="s-f6af3c6655"></a>`requires_python` | >=3.12 |
| <a id="s-7da1529b8a"></a>`role` | reusable_library |
| <a id="s-69e1df2c1a"></a>`source` | reference/stove0/packages/protocol/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-protocol](../../../evidence/relationships.md#rn-72232b7fce)

## Governing policies

- <a id="pa-4a44b3428f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-protocol](../../../evidence/sources.md#src-8793f1ad67) — `reference/stove0/packages/protocol/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-protocol`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4509483ec6c9fcc69d1d34ecc90d4316c177822b3f64e30f4c33c56e330e3e91 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Canonical content-opaque collection orchestration contracts for stove0.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/protocol/pyproject.toml"
}
```
