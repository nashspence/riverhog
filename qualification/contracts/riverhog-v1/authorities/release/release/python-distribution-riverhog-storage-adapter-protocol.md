# Python distribution: riverhog-storage-adapter-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-storage-adap-1c35ec3ef1:700dbcb513 -->

Provider-neutral opaque-object capability contracts for Riverhog storage adapters.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-5e597a2223"></a>
| Concern | Contract |
|---|---|
| <a id="s-4f7caad5bf"></a>`artifacts` | [{"coordinate": "dist/riverhog_storage_adapter_protocol-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_storage_adapter_protocol-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-9bbb9f09a8"></a>`channel` | github-release |
| <a id="s-725e6a547d"></a>`description` | Provider-neutral opaque-object capability contracts for Riverhog storage adapters. |
| <a id="s-375214dae6"></a>`requires_python` | >=3.12 |
| <a id="s-b0785ae047"></a>`role` | reusable_library |
| <a id="s-04c9638a8a"></a>`source` | packages/riverhog-storage-adapter-protocol/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-protocol](../../../evidence/relationships.md#rn-e65e47e332)

## Governing policies

- <a id="pa-48ed182fa1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-storage-adapter-protocol](../../../evidence/sources.md#src-986408de09) — `packages/riverhog-storage-adapter-protocol/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-protocol`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c965eae12774fe716609c0183e40efb5ac4e8bb875f6b0fbb7a9ac2a59b1b80 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_storage_adapter_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_storage_adapter_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Provider-neutral opaque-object capability contracts for Riverhog storage adapters.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-storage-adapter-protocol/pyproject.toml"
}
```
