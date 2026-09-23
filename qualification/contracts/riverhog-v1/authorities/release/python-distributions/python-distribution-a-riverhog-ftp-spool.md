# Python distribution: a-riverhog-ftp-spool

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-ftp-spool:c09488b9fb -->

FTP upload spool and ingestion adapter for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-c12f26f3d1"></a>
| Concern | Contract |
|---|---|
| <a id="s-8f7a11ef14"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_ftp_spool-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_ftp_spool-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-887f5305a0"></a>`channel` | `"github-release"` |
| <a id="s-a1354e7b05"></a>`description` | `"FTP upload spool and ingestion adapter for Riverhog."` |
| <a id="s-7fb37fba2d"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-ba150b903e"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-cdb8502990"></a>`publication_identity` | `{"coordinate":"a-riverhog-ftp-spool","kind":"python-distribution"}` |
| <a id="s-c6ac438434"></a>`requires_python` | `">=3.12"` |
| <a id="s-f42ce70eb3"></a>`role` | `"component"` |
| <a id="s-9979bf377f"></a>`source` | `"some-implementations/riverhog/ingress/ftp/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-ftp-spool](../../../evidence/relationships/nodes.md#rn-507e6fb957)

## Governing policies

- <a id="pa-b4064c80a1"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-f3f17964e0"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-d46972a171) — [some-implementations/riverhog/ingress/ftp/pyproject.toml](../../../../../../some-implementations/riverhog/ingress/ftp/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-ftp-spool`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a518292ce09bbd691620252e6fb88cedbd75e6ced50e6ee34aa9f96d136ba997 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_ftp_spool-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_ftp_spool-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "FTP upload spool and ingestion adapter for Riverhog.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-ftp-spool",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/ingress/ftp/pyproject.toml"
}
```

</details>
