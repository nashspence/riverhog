# Python distribution: a-gogurt-path-volume-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-gogurt-path-volume-lib:f65e965a70 -->

Path-mounted volume library for Gogurt providers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-4a620873b9"></a>
| Concern | Contract |
|---|---|
| <a id="s-8de890d9f1"></a>`artifacts` | `[{"coordinate":"dist/a_gogurt_path_volume_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_gogurt_path_volume_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-cf404f174e"></a>`channel` | `"github-release"` |
| <a id="s-072e96ff40"></a>`description` | `"Path-mounted volume library for Gogurt providers."` |
| <a id="s-30f34154a6"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-65460861b5"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-ff8e549cf9"></a>`publication_identity` | `{"coordinate":"a-gogurt-path-volume-lib","kind":"python-distribution"}` |
| <a id="s-678907e691"></a>`requires_python` | `">=3.12"` |
| <a id="s-611b00c376"></a>`role` | `"reusable_library"` |
| <a id="s-78fedcf081"></a>`source` | `"some-implementations/gogurt/mounted-volume/path-support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-gogurt-path-volume-lib](../../../evidence/relationships/nodes.md#rn-a80bf131fc)

## Governing policies

- <a id="pa-f62e0c3523"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-5fddbac75b"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-gogurt-path-volume-lib](../../../evidence/sources/authorities.md#src-73b5b2f64c) — [some-implementations/gogurt/mounted-volume/path-support/pyproject.toml](../../../../../../some-implementations/gogurt/mounted-volume/path-support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-gogurt-path-volume-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc94abec3dba1d17afadaedf15e3e24d70c09175268564f8ca11ed4b5c296c09 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_gogurt_path_volume_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_gogurt_path_volume_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Path-mounted volume library for Gogurt providers.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-gogurt-path-volume-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/gogurt/mounted-volume/path-support/pyproject.toml"
}
```

</details>
