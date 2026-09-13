# Python distribution: riverhog-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-protocol:fb89a12add -->

Canonical Riverhog wire and identity contracts.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-4440275f3d"></a>
| Concern | Contract |
|---|---|
| <a id="s-9d441f9d70"></a>`artifacts` | [{"coordinate": "dist/riverhog_protocol-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_protocol-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-ebd2940fb5"></a>`channel` | github-release |
| <a id="s-0f222c026a"></a>`description` | Canonical Riverhog wire and identity contracts. |
| <a id="s-09cd13996c"></a>`requires_python` | >=3.12 |
| <a id="s-6aedf80100"></a>`role` | reusable_library |
| <a id="s-fba5b34eda"></a>`source` | packages/riverhog-protocol/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-protocol](../../../evidence/relationships.md#rn-20dbb0d5c0)

## Governing policies

- <a id="pa-b12eff71fc"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-a2ddde29ec"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-protocol](../../../evidence/sources.md#src-1221d32c9e) — `packages/riverhog-protocol/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-protocol`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75a81e8ff102dff823ccdf8170a05e9418a5e2838fb649e366cdc7ea13afc1fe -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Canonical Riverhog wire and identity contracts.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-protocol/pyproject.toml"
}
```
