# Python distribution: a-riverhog-ftp-spool-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-ftp-spool-client:8ae10e2311 -->

Client for the Riverhog FTP upload spool.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-2f2b089f85"></a>
| Concern | Contract |
|---|---|
| <a id="s-6f2ba8f114"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_ftp_spool_client-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_ftp_spool_client-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-d58056ee0f"></a>`channel` | `"github-release"` |
| <a id="s-992c6c7bdb"></a>`description` | `"Client for the Riverhog FTP upload spool."` |
| <a id="s-190b129197"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-02e4c7be91"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-bf92a4a69a"></a>`publication_identity` | `{"coordinate":"a-riverhog-ftp-spool-client","kind":"python-distribution"}` |
| <a id="s-9e9b8c1a91"></a>`requires_python` | `">=3.12"` |
| <a id="s-a302b8e42c"></a>`role` | `"component"` |
| <a id="s-fd284ffcf3"></a>`source` | `"some-implementations/riverhog/ingress/ftp-api-client/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-ftp-spool-client](../../../evidence/relationships/nodes.md#rn-b9a37d9110)

## Governing policies

- <a id="pa-e33a680b76"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-31e28a0757"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-ftp-spool-client](../../../evidence/sources/authorities.md#src-951f173e46) — [some-implementations/riverhog/ingress/ftp-api-client/pyproject.toml](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-ftp-spool-client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea2c52cb45c2bd0d275d0a9b7bdebd3be6d9b92a81fa7b3864fb06bf1b7abba9 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_ftp_spool_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_ftp_spool_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Client for the Riverhog FTP upload spool.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-ftp-spool-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/ingress/ftp-api-client/pyproject.toml"
}
```

</details>
