# Python distribution: a-riverhog-s3-store-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-s3-store-lib:b46fb85cc5 -->

S3 store library for Riverhog storage adapters.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-767fe58893"></a>
| Concern | Contract |
|---|---|
| <a id="s-eca37f902e"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_s3_store_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_s3_store_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-14b57727e3"></a>`channel` | `"github-release"` |
| <a id="s-333508c1fe"></a>`description` | `"S3 store library for Riverhog storage adapters."` |
| <a id="s-74d034abca"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-24fec51e30"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-e10d26f334"></a>`publication_identity` | `{"coordinate":"a-riverhog-s3-store-lib","kind":"python-distribution"}` |
| <a id="s-f47686425c"></a>`requires_python` | `">=3.12"` |
| <a id="s-b23ec564f1"></a>`role` | `"reusable_library"` |
| <a id="s-4d148a8292"></a>`source` | `"some-implementations/riverhog/storage/s3-support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-s3-store-lib](../../../evidence/relationships/nodes.md#rn-312432d469)

## Governing policies

- <a id="pa-24f875dca7"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-a204b6d288"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-s3-store-lib](../../../evidence/sources/authorities.md#src-da72f6a598) — [some-implementations/riverhog/storage/s3-support/pyproject.toml](../../../../../../some-implementations/riverhog/storage/s3-support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-s3-store-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7052308de406fd1d54aae660027e381a46d2c1b9a2fad2ee15642e62faf72196 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_s3_store_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_s3_store_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "S3 store library for Riverhog storage adapters.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-s3-store-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/riverhog/storage/s3-support/pyproject.toml"
}
```

</details>
