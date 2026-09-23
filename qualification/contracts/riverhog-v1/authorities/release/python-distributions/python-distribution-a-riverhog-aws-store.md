# Python distribution: a-riverhog-aws-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-aws-store:4c72de74ce -->

AWS-backed Riverhog archive and retrieval store.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-eaf7eaf2eb"></a>
| Concern | Contract |
|---|---|
| <a id="s-d5c239a249"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_aws_store-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_aws_store-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-5bc4842337"></a>`channel` | `"github-release"` |
| <a id="s-de55cefd54"></a>`description` | `"AWS-backed Riverhog archive and retrieval store."` |
| <a id="s-def3bd02bc"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-3dbcea7ab4"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-ee45761d31"></a>`publication_identity` | `{"coordinate":"a-riverhog-aws-store","kind":"python-distribution"}` |
| <a id="s-ba4fa121c3"></a>`requires_python` | `">=3.12"` |
| <a id="s-8d684d7615"></a>`role` | `"component"` |
| <a id="s-5a7c6cc296"></a>`source` | `"some-implementations/riverhog/storage/aws/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-aws-store](../../../evidence/relationships/nodes.md#rn-b19c22f180)

## Governing policies

- <a id="pa-127cde47f9"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-84283c8bba"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-aws-store](../../../evidence/sources/authorities.md#src-e0641d1174) — [some-implementations/riverhog/storage/aws/pyproject.toml](../../../../../../some-implementations/riverhog/storage/aws/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-aws-store`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0175f9be2013d500ac025d655fd497303c83ec092077e6766c61bc50d37af92f -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_aws_store-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_aws_store-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "AWS-backed Riverhog archive and retrieval store.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-riverhog-aws-store",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/storage/aws/pyproject.toml"
}
```

</details>
