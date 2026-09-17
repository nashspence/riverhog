# Python distribution: riverhog-ftp-adapter-api-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-ftp-adapter-api-client:c3c61ea680 -->

Optional nonnormative client for the Riverhog FTP ingress reference.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-9b6200f25e"></a>
| Concern | Contract |
|---|---|
| <a id="s-ffe3255b10"></a>`artifacts` | `[{"coordinate":"dist/riverhog_ftp_adapter_api_client-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_ftp_adapter_api_client-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-f28ef68e25"></a>`channel` | `"github-release"` |
| <a id="s-075d805e40"></a>`description` | `"Optional nonnormative client for the Riverhog FTP ingress reference."` |
| <a id="s-d0acdf17f0"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-cafb1942b6"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-04f9fd8092"></a>`publication_identity` | `{"coordinate":"riverhog-ftp-adapter-api-client","kind":"python-distribution"}` |
| <a id="s-f1f2c566f2"></a>`requires_python` | `">=3.12"` |
| <a id="s-2110d96cc1"></a>`role` | `"reference_component"` |
| <a id="s-76d81eaf74"></a>`source` | `"reference/riverhog/ingress/ftp-api-client/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-ftp-adapter-api-client](../../../evidence/relationships/nodes.md#rn-5044d995ba)

## Governing policies

- <a id="pa-c3b2465afd"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-0a80996900"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-ftp-adapter-api-client](../../../evidence/sources/authorities.md#src-70a41aeb48) — [reference/riverhog/ingress/ftp-api-client/pyproject.toml](../../../../../../reference/riverhog/ingress/ftp-api-client/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-ftp-adapter-api-client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9af103331afe83e2d351e1285f86c307c85e407a52810ebb03b6bdf083f9d304 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_ftp_adapter_api_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_ftp_adapter_api_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative client for the Riverhog FTP ingress reference.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-ftp-adapter-api-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/ingress/ftp-api-client/pyproject.toml"
}
```

</details>
