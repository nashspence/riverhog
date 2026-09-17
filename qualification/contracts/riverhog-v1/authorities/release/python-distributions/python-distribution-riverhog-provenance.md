# Python distribution: riverhog-provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-provenance:a671cd43a2 -->

Portable Riverhog v1 per-file provenance journals and validation.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-cfee2f0d94"></a>
| Concern | Contract |
|---|---|
| <a id="s-53dcd05eda"></a>`artifacts` | `[{"coordinate":"dist/riverhog_provenance-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_provenance-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-8c3203294a"></a>`channel` | `"github-release"` |
| <a id="s-ff0786b31c"></a>`description` | `"Portable Riverhog v1 per-file provenance journals and validation."` |
| <a id="s-4b753f664a"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-a9f5e31450"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-ddf33a08a6"></a>`publication_identity` | `{"coordinate":"riverhog-provenance","kind":"python-distribution"}` |
| <a id="s-2f0919c65a"></a>`requires_python` | `">=3.12"` |
| <a id="s-68a74a45c0"></a>`role` | `"reusable_library"` |
| <a id="s-dd34473425"></a>`source` | `"packages/riverhog-provenance/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance](../../../evidence/relationships/nodes.md#rn-728e08e7c4)

## Governing policies

- <a id="pa-43c70c3078"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-86f2679743"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-provenance](../../../evidence/sources/authorities.md#src-95dbd50af1) — [packages/riverhog-provenance/pyproject.toml](../../../../../../packages/riverhog-provenance/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b079e1e121ff5c9c8406969f564dcd5f655c8feb923928a7d49b42a8190ff897 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Portable Riverhog v1 per-file provenance journals and validation.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-provenance",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-provenance/pyproject.toml"
}
```

</details>
