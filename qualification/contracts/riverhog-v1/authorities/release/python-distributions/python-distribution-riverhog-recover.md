# Python distribution: riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-recover:90dcfd9e14 -->

Optional nonnormative independent recovery reference application for Riverhog archives.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-4f2ec1b9f5"></a>
| Concern | Contract |
|---|---|
| <a id="s-aae98b1c04"></a>`artifacts` | `[{"coordinate":"dist/riverhog_recover-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_recover-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-cf1f08603c"></a>`channel` | `"github-release"` |
| <a id="s-52c6490cac"></a>`description` | `"Optional nonnormative independent recovery reference application for Riverhog archives."` |
| <a id="s-827530076d"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-e554dc159c"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-bae245f510"></a>`publication_identity` | `{"coordinate":"riverhog-recover","kind":"python-distribution"}` |
| <a id="s-7e5bc2c17e"></a>`requires_python` | `">=3.12"` |
| <a id="s-f11df5134b"></a>`role` | `"reference_application"` |
| <a id="s-426e309e01"></a>`source` | `"reference/riverhog/recovery/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-recover](../../../evidence/relationships.md#rn-813d97e5a4)

## Governing policies

- <a id="pa-0831081fc4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-03b58677d8"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-recover](../../../evidence/sources.md#src-917183ebd1) — `reference/riverhog/recovery/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-recover`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86b8bb5426086ded476949a1edd2c64f6c32fad8d0420d42de5ed375a730fe11 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_recover-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_recover-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative independent recovery reference application for Riverhog archives.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-recover",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_application",
  "source": "reference/riverhog/recovery/pyproject.toml"
}
```

</details>
