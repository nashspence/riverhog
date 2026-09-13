# Python distribution: riverhog-storage-adapter-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-storage-adapter-support:80c3a7ecc8 -->

HTTP binding and conformance support for Riverhog storage adapters.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-ef8232e452"></a>
| Concern | Contract |
|---|---|
| <a id="s-0071f98da8"></a>`artifacts` | [{"coordinate": "dist/riverhog_storage_adapter_support-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_storage_adapter_support-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-82d50ec7a0"></a>`channel` | github-release |
| <a id="s-308ee021f4"></a>`description` | HTTP binding and conformance support for Riverhog storage adapters. |
| <a id="s-cd8c7cb430"></a>`requires_python` | >=3.12 |
| <a id="s-1796668523"></a>`role` | reusable_library |
| <a id="s-1a9dc4c79f"></a>`source` | packages/riverhog-storage-adapter-support/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-support](../../../evidence/relationships.md#rn-f0c3b34058)

## Governing policies

- <a id="pa-70fc3de171"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-storage-adapter-support](../../../evidence/sources.md#src-e3b24ac45f) — `packages/riverhog-storage-adapter-support/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-support`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d65252db4a7b28127c50498777179540468e162cf6cf1c8388ea84819ae091d5 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_storage_adapter_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_storage_adapter_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "HTTP binding and conformance support for Riverhog storage adapters.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-storage-adapter-support/pyproject.toml"
}
```
