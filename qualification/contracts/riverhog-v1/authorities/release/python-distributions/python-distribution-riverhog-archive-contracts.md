# Python distribution: riverhog-archive-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-archive-contracts:446f66d028 -->

Dependency-light immutable Riverhog archive recovery contracts.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-837b02f080"></a>
| Concern | Contract |
|---|---|
| <a id="s-17f3d8f95c"></a>`artifacts` | `[{"coordinate":"dist/riverhog_archive_contracts-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_archive_contracts-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-378cb2b77a"></a>`channel` | `"github-release"` |
| <a id="s-2f281fd71a"></a>`description` | `"Dependency-light immutable Riverhog archive recovery contracts."` |
| <a id="s-52126ee826"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-2ee4305e45"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-fd0558a629"></a>`publication_identity` | `{"coordinate":"riverhog-archive-contracts","kind":"python-distribution"}` |
| <a id="s-c6c6acde15"></a>`requires_python` | `">=3.12"` |
| <a id="s-1e476313d3"></a>`role` | `"reusable_library"` |
| <a id="s-19475f5c7c"></a>`source` | `"packages/riverhog-archive-contracts/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-archive-contracts](../../../evidence/relationships/nodes.md#rn-3f00f69253)

## Governing policies

- <a id="pa-2aec1a8f1c"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-a30ae5282a"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-archive-contracts](../../../evidence/sources/authorities.md#src-f6ac304b52) — [packages/riverhog-archive-contracts/pyproject.toml](../../../../../../packages/riverhog-archive-contracts/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-archive-contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9a554ebb83a0686849584dd4b27618f1ef06dd14003ba41c25545691cffc05d -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_archive_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_archive_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Dependency-light immutable Riverhog archive recovery contracts.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-archive-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-archive-contracts/pyproject.toml"
}
```

</details>
