# Python distribution: riverhog-storage-adapter-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-storage-adapter-support:bef8a7f11f -->

HTTP binding and conformance support for Riverhog storage adapters.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ef8232e452"></a>
| Concern | Contract |
|---|---|
| <a id="s-0071f98da8"></a>`artifacts` | `[{"coordinate":"dist/riverhog_storage_adapter_support-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_storage_adapter_support-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-82d50ec7a0"></a>`channel` | `"github-release"` |
| <a id="s-308ee021f4"></a>`description` | `"HTTP binding and conformance support for Riverhog storage adapters."` |
| <a id="s-ad86bba8de"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-b6f3c9aa64"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-25f054a1d0"></a>`publication_identity` | `{"coordinate":"riverhog-storage-adapter-support","kind":"python-distribution"}` |
| <a id="s-cd8c7cb430"></a>`requires_python` | `">=3.12"` |
| <a id="s-1796668523"></a>`role` | `"reusable_library"` |
| <a id="s-1a9dc4c79f"></a>`source` | `"packages/riverhog-storage-adapter-support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-support](../../../evidence/relationships.md#rn-f0c3b34058)

## Governing policies

- <a id="pa-5bd865c32c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0e397b4cd9"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2dba59f1cc3c9f53c8eec7d3dc0d8619433fc91406994ceac010ac8cea573854 -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-storage-adapter-support",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-storage-adapter-support/pyproject.toml"
}
```

</details>
