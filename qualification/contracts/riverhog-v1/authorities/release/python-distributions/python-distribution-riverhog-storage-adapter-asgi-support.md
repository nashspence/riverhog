# Python distribution: riverhog-storage-adapter-asgi-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-storage-adap-32de72f8fe:5b4164aa32 -->

Authenticated ASGI shell for independently scoped Riverhog storage adapters.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-6c7d9b3e6d"></a>
| Concern | Contract |
|---|---|
| <a id="s-c76771e8a5"></a>`artifacts` | [{"coordinate": "dist/riverhog_storage_adapter_asgi_support-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_storage_adapter_asgi_support-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-a12992c423"></a>`channel` | github-release |
| <a id="s-30fca72602"></a>`description` | Authenticated ASGI shell for independently scoped Riverhog storage adapters. |
| <a id="s-ca8f33d833"></a>`requires_python` | >=3.12 |
| <a id="s-e2737eada4"></a>`role` | reusable_library |
| <a id="s-af217de559"></a>`source` | packages/riverhog-storage-adapter-asgi-support/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-asgi-support](../../../evidence/relationships.md#rn-922b8ba508)

## Governing policies

- <a id="pa-709c61ea1e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-77e2bc4a9a"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-storage-adapter-asgi-support](../../../evidence/sources.md#src-f4e68c2bf9) — `packages/riverhog-storage-adapter-asgi-support/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-asgi-support`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3bf004177dd691c896fb61fa9c0a0c7ddafe3fc145efb352283e8188cfdacb2 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_storage_adapter_asgi_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_storage_adapter_asgi_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Authenticated ASGI shell for independently scoped Riverhog storage adapters.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-storage-adapter-asgi-support/pyproject.toml"
}
```
