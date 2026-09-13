# Python distribution: riverhog-ftp-adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-ftp-adapter:18a5e3b53e -->

Optional nonnormative FTP ingress reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-53b8602e73"></a>
| Concern | Contract |
|---|---|
| <a id="s-6b26928f53"></a>`artifacts` | [{"coordinate": "dist/riverhog_ftp_adapter-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_ftp_adapter-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-dec2428ab7"></a>`channel` | github-release |
| <a id="s-66a2aeffc7"></a>`description` | Optional nonnormative FTP ingress reference for Riverhog. |
| <a id="s-ceaae5ffc7"></a>`requires_python` | >=3.12 |
| <a id="s-fc6830a1aa"></a>`role` | reference_component |
| <a id="s-35cbf8b739"></a>`source` | reference/riverhog/ingress/ftp/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-ftp-adapter](../../../evidence/relationships.md#rn-d738980294)

## Governing policies

- <a id="pa-facda61101"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8becaf0779"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-ftp-adapter](../../../evidence/sources.md#src-37bda529c9) — `reference/riverhog/ingress/ftp/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-ftp-adapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf58fbea4d468f046def867aa2a0a2fcb7748b37db244f3eec27dc7c1001f498 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_ftp_adapter-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_ftp_adapter-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative FTP ingress reference for Riverhog.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/ingress/ftp/pyproject.toml"
}
```
