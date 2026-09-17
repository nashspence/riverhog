# Python distribution: riverhog-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-client:902b456afc -->

Typed generic Riverhog client and capability-scoped collection-processing runtime.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ccd6b35591"></a>
| Concern | Contract |
|---|---|
| <a id="s-f059b1d319"></a>`artifacts` | `[{"coordinate":"dist/riverhog_client-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_client-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-0d444d3d41"></a>`channel` | `"github-release"` |
| <a id="s-2f22635653"></a>`description` | `"Typed generic Riverhog client and capability-scoped collection-processing runtime."` |
| <a id="s-7b2c8a3e64"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-4d3aaf2875"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-2b65ac7822"></a>`publication_identity` | `{"coordinate":"riverhog-client","kind":"python-distribution"}` |
| <a id="s-ab052b72f0"></a>`requires_python` | `">=3.12"` |
| <a id="s-a158229f6f"></a>`role` | `"reusable_library"` |
| <a id="s-1bf047b001"></a>`source` | `"packages/riverhog-client/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-client](../../../evidence/relationships/nodes.md#rn-8e5274344f)

## Governing policies

- <a id="pa-35c83822fe"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-c6cfef82a6"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-client](../../../evidence/sources/authorities.md#src-7d25f2c873) — [packages/riverhog-client/pyproject.toml](../../../../../../packages/riverhog-client/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5366d6e05e6ec6e89c20f6d3bd9cb9425026a729e5dfe67125d6b6f4f449869 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Typed generic Riverhog client and capability-scoped collection-processing runtime.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-client/pyproject.toml"
}
```

</details>
