# Python distribution: a-riverhog-macos-provenance-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-macos-prov-738960f248:f5ec4ae671 -->

macOS filesystem observer for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ab0200cbb9"></a>
| Concern | Contract |
|---|---|
| <a id="s-132be14874"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_macos_provenance_observer-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_macos_provenance_observer-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-3d12e7ceea"></a>`channel` | `"github-release"` |
| <a id="s-765a32a282"></a>`description` | `"macOS filesystem observer for Riverhog provenance."` |
| <a id="s-5ec6e93174"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-be3171d802"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-fea81563ef"></a>`publication_identity` | `{"coordinate":"a-riverhog-macos-provenance-observer","kind":"python-distribution"}` |
| <a id="s-363d4609da"></a>`requires_python` | `">=3.12"` |
| <a id="s-0894be8731"></a>`role` | `"component"` |
| <a id="s-a3cb44f8be"></a>`source` | `"some-implementations/riverhog/provenance/observers/macos/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-macos-provenance-observer](../../../evidence/relationships/nodes.md#rn-eef8dc0e28)

## Governing policies

- <a id="pa-06d4e7ba5a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-9b86fb153c"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-macos-provenance-observer](../../../evidence/sources/authorities.md#src-a5aa3a7898) — [some-implementations/riverhog/provenance/observers/macos/pyproject.toml](../../../../../../some-implementations/riverhog/provenance/observers/macos/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-macos-provenance-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d06fafcbe19c9d25c65d804af944a69324eb8f6d4d3ef41ba07431654d8668e5 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_macos_provenance_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_macos_provenance_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "macOS filesystem observer for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-macos-provenance-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/riverhog/provenance/observers/macos/pyproject.toml"
}
```

</details>
