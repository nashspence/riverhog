# Python distribution: a-riverhog-filesystem-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-filesystem-store:540d81387c -->

Filesystem-backed Riverhog archive and retrieval store.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-2b10f6d387"></a>
| Concern | Contract |
|---|---|
| <a id="s-1f24a38852"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_filesystem_store-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_filesystem_store-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-d3a58d1d2a"></a>`channel` | `"github-release"` |
| <a id="s-5c51f75f36"></a>`description` | `"Filesystem-backed Riverhog archive and retrieval store."` |
| <a id="s-47c555f76e"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-423739dbd7"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-c80dba3ea4"></a>`publication_identity` | `{"coordinate":"a-riverhog-filesystem-store","kind":"python-distribution"}` |
| <a id="s-bcee60ba42"></a>`requires_python` | `">=3.12"` |
| <a id="s-d7fd4bc2f2"></a>`role` | `"component"` |
| <a id="s-cc2f07f79b"></a>`source` | `"some-implementations/riverhog/storage/filesystem/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-filesystem-store](../../../evidence/relationships/nodes.md#rn-b69efa5ec4)

## Governing policies

- <a id="pa-dbf4acf5ce"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-24c0930044"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-filesystem-store](../../../evidence/sources/authorities.md#src-04ec1f9388) — [some-implementations/riverhog/storage/filesystem/pyproject.toml](../../../../../../some-implementations/riverhog/storage/filesystem/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-filesystem-store`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f7be9314a87919abd83130917d4d8444ca5c9f2099a539f85687413212ae8e36 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_filesystem_store-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_filesystem_store-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Filesystem-backed Riverhog archive and retrieval store.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-riverhog-filesystem-store",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/storage/filesystem/pyproject.toml"
}
```

</details>
