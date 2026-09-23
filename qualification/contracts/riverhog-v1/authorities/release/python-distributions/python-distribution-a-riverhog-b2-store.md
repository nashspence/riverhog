# Python distribution: a-riverhog-b2-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-b2-store:63497b0f7d -->

Backblaze B2-backed Riverhog archive and retrieval store.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-9573427a52"></a>
| Concern | Contract |
|---|---|
| <a id="s-1bbfcd50f9"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_b2_store-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_b2_store-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-910724ce13"></a>`channel` | `"github-release"` |
| <a id="s-3734d70215"></a>`description` | `"Backblaze B2-backed Riverhog archive and retrieval store."` |
| <a id="s-bcfcaadc98"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-72c1c50a24"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-95e70aa806"></a>`publication_identity` | `{"coordinate":"a-riverhog-b2-store","kind":"python-distribution"}` |
| <a id="s-b4a8147ae1"></a>`requires_python` | `">=3.12"` |
| <a id="s-7b6d130600"></a>`role` | `"component"` |
| <a id="s-7950e26094"></a>`source` | `"some-implementations/riverhog/storage/backblaze/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-b2-store](../../../evidence/relationships/nodes.md#rn-6727835d85)

## Governing policies

- <a id="pa-ded3201761"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-1ba84ce9d7"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-b2-store](../../../evidence/sources/authorities.md#src-d9c3cc5b94) — [some-implementations/riverhog/storage/backblaze/pyproject.toml](../../../../../../some-implementations/riverhog/storage/backblaze/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-b2-store`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8059f4f04b784caeed1c7a57a0c254371314e451379916ea6213d9260ee6ac36 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_b2_store-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_b2_store-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Backblaze B2-backed Riverhog archive and retrieval store.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-riverhog-b2-store",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/storage/backblaze/pyproject.toml"
}
```

</details>
