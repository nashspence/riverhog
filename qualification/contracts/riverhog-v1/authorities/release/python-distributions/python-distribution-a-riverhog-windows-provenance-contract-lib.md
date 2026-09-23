# Python distribution: a-riverhog-windows-provenance-contract-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-windows-pr-fbe753eef3:77bb6f8061 -->

Windows filesystem observation contracts for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ff565182b3"></a>
| Concern | Contract |
|---|---|
| <a id="s-64ea987e38"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_windows_provenance_contract_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_windows_provenance_contract_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-619c294a6d"></a>`channel` | `"github-release"` |
| <a id="s-631f05ef0c"></a>`description` | `"Windows filesystem observation contracts for Riverhog provenance."` |
| <a id="s-15a1bb83ac"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-3e714e1c4c"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-2feca52231"></a>`publication_identity` | `{"coordinate":"a-riverhog-windows-provenance-contract-lib","kind":"python-distribution"}` |
| <a id="s-e7523cc724"></a>`requires_python` | `">=3.12"` |
| <a id="s-25b3f9d370"></a>`role` | `"component"` |
| <a id="s-735fa05a84"></a>`source` | `"some-implementations/riverhog/provenance/contracts/windows/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-windows-provenance-contract-lib](../../../evidence/relationships/nodes.md#rn-19250524ad)

## Governing policies

- <a id="pa-5a453536b2"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-58dddaf81f"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-windows-provenance-contract-lib](../../../evidence/sources/authorities.md#src-f484074408) — [some-implementations/riverhog/provenance/contracts/windows/pyproject.toml](../../../../../../some-implementations/riverhog/provenance/contracts/windows/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-windows-provenance-contract-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d30b50d5553f87e913b01b5ba00ead69ca87e79c0f65d7d6c0c0de31c7b750fa -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_windows_provenance_contract_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_windows_provenance_contract_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Windows filesystem observation contracts for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-windows-provenance-contract-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/provenance/contracts/windows/pyproject.toml"
}
```

</details>
