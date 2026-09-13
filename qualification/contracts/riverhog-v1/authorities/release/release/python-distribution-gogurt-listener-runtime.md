# Python distribution: gogurt-listener-runtime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-gogurt-listener-runtime:e7a7c41b5a -->

Portable durable listener runtime and native-platform port for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-e542fe29c9"></a>
| Concern | Contract |
|---|---|
| <a id="s-e6233d3b53"></a>`artifacts` | [{"coordinate": "dist/gogurt_listener_runtime-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_listener_runtime-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-3150381b0f"></a>`channel` | github-release |
| <a id="s-fc32147bcb"></a>`description` | Portable durable listener runtime and native-platform port for Gogurt. |
| <a id="s-6cdafa600f"></a>`requires_python` | >=3.12 |
| <a id="s-30723da06a"></a>`role` | reusable_library |
| <a id="s-786d10abd4"></a>`source` | reference/gogurt/packages/listener-runtime/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-listener-runtime](../../../evidence/relationships.md#rn-df1cdfd17c)

## Governing policies

- <a id="pa-158617256b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-listener-runtime](../../../evidence/sources.md#src-23725dce0b) — `reference/gogurt/packages/listener-runtime/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-listener-runtime`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad382999491b93fe58fc20503ffdd143e69c373eacf3796b9dc88eee27a2b52d -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_listener_runtime-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_listener_runtime-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Portable durable listener runtime and native-platform port for Gogurt.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/gogurt/packages/listener-runtime/pyproject.toml"
}
```
