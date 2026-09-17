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
| <a id="s-9d441f9d70"></a>`artifacts` | `[{"coordinate":"dist/riverhog_protocol-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_protocol-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-ebd2940fb5"></a>`channel` | `"github-release"` |
| <a id="s-0f222c026a"></a>`description` | `"Canonical Riverhog wire and identity contracts."` |
| <a id="s-6695d21858"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-0a152cff8b"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-f232b42c56"></a>`publication_identity` | `{"coordinate":"riverhog-protocol","kind":"python-distribution"}` |
| <a id="s-09cd13996c"></a>`requires_python` | `">=3.12"` |
| <a id="s-6aedf80100"></a>`role` | `"reusable_library"` |
| <a id="s-fba5b34eda"></a>`source` | `"packages/riverhog-protocol/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-protocol](../../../evidence/relationships/nodes.md#rn-20dbb0d5c0)

## Governing policies

- <a id="pa-b12eff71fc"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-a2ddde29ec"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-protocol](../../../evidence/sources/authorities.md#src-1221d32c9e) — [packages/riverhog-protocol/pyproject.toml](../../../../../../packages/riverhog-protocol/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-protocol`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f9d1907d35ec3860447ed9cc100742bede075e20741cce1ce4d18fe130eef8e -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-protocol",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-protocol/pyproject.toml"
}
```

</details>
