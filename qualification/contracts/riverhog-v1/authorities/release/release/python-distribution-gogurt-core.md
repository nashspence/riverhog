# Python distribution: gogurt-core

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-gogurt-core:a465f4552f -->

Portable Gogurt marker, routing, action, and watch semantics.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-d12fd7d1d5"></a>
| Concern | Contract |
|---|---|
| <a id="s-4ea058acac"></a>`artifacts` | [{"coordinate": "dist/gogurt_core-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_core-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-c6752b84c5"></a>`channel` | github-release |
| <a id="s-90f8392ab0"></a>`description` | Portable Gogurt marker, routing, action, and watch semantics. |
| <a id="s-1a41c5d00c"></a>`requires_python` | >=3.12 |
| <a id="s-01b66a1a3a"></a>`role` | reusable_library |
| <a id="s-7f695858d5"></a>`source` | reference/gogurt/packages/core/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-core](../../../evidence/relationships.md#rn-e1d2ac7df3)

## Governing policies

- <a id="pa-ef7eb568b0"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-core](../../../evidence/sources.md#src-2850fdf46b) — `reference/gogurt/packages/core/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-core`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09d2a32b679b743b571ba142898e0bd984ddc8b27b1317a561fcf709b007b719 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_core-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_core-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Portable Gogurt marker, routing, action, and watch semantics.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/gogurt/packages/core/pyproject.toml"
}
```
