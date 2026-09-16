# Python distribution: riverhog-age

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-age:20fb8599d2 -->

Resumable age encryption used by the Riverhog protocol.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-f7b2c30387"></a>
| Concern | Contract |
|---|---|
| <a id="s-a58da95517"></a>`artifacts` | `[{"coordinate":"dist/riverhog_age-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_age-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-945c8f35d5"></a>`channel` | `"github-release"` |
| <a id="s-c2f401a636"></a>`description` | `"Resumable age encryption used by the Riverhog protocol."` |
| <a id="s-531de12b58"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-77f7a99fc3"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-8e481a5ab0"></a>`publication_identity` | `{"coordinate":"riverhog-age","kind":"python-distribution"}` |
| <a id="s-7c6d7f6f04"></a>`requires_python` | `">=3.12"` |
| <a id="s-cd5997ed2c"></a>`role` | `"reusable_library"` |
| <a id="s-8a440f3e60"></a>`source` | `"packages/riverhog-age/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-age](../../../evidence/relationships.md#rn-24025966b4)

## Governing policies

- <a id="pa-2b5725b502"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-93ea2391ee"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-age](../../../evidence/sources.md#src-2d271f6b70) — `packages/riverhog-age/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-age`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91a9918c001b34b23e6a1cf3caaf36fa35dfdcb1391fbc8484ed7feb7d57d549 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_age-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_age-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Resumable age encryption used by the Riverhog protocol.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-age",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-age/pyproject.toml"
}
```

</details>
