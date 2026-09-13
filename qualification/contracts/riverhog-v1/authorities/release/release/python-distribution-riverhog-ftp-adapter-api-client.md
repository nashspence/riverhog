# Python distribution: riverhog-ftp-adapter-api-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-ftp-adapter-api-client:fdeec3b0be -->

Optional nonnormative client for the Riverhog FTP ingress reference.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-9b6200f25e"></a>
| Concern | Contract |
|---|---|
| <a id="s-ffe3255b10"></a>`artifacts` | [{"coordinate": "dist/riverhog_ftp_adapter_api_client-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_ftp_adapter_api_client-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-f28ef68e25"></a>`channel` | github-release |
| <a id="s-075d805e40"></a>`description` | Optional nonnormative client for the Riverhog FTP ingress reference. |
| <a id="s-f1f2c566f2"></a>`requires_python` | >=3.12 |
| <a id="s-2110d96cc1"></a>`role` | reference_component |
| <a id="s-76d81eaf74"></a>`source` | reference/riverhog/ingress/ftp-api-client/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-ftp-adapter-api-client](../../../evidence/relationships.md#rn-5044d995ba)

## Governing policies

- <a id="pa-9f22731d77"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-ftp-adapter-api-client](../../../evidence/sources.md#src-70a41aeb48) — `reference/riverhog/ingress/ftp-api-client/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-ftp-adapter-api-client`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5aa8ad488fa7e59ac76d10b92dae7b1a0a996e4bfd4d0e191dce3c37d1e5295a -->

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
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/ingress/ftp-api-client/pyproject.toml"
}
```
