# Python distribution: a-riverhog-recovery-tool

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-recovery-tool:a9caf098d2 -->

Independent recovery tool for Riverhog archives.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-c09ef40ba8"></a>
| Concern | Contract |
|---|---|
| <a id="s-8fcf7e99ce"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_recovery_tool-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_recovery_tool-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-74dba492c8"></a>`channel` | `"github-release"` |
| <a id="s-504daee7d6"></a>`description` | `"Independent recovery tool for Riverhog archives."` |
| <a id="s-6fdcc98f07"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-84f3d691eb"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-481db580d7"></a>`publication_identity` | `{"coordinate":"a-riverhog-recovery-tool","kind":"python-distribution"}` |
| <a id="s-2de941329f"></a>`requires_python` | `">=3.12"` |
| <a id="s-43d5dd1e91"></a>`role` | `"application"` |
| <a id="s-8b78ee3130"></a>`source` | `"some-implementations/riverhog/recovery/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-recovery-tool](../../../evidence/relationships/nodes.md#rn-418e26fced)

## Governing policies

- <a id="pa-3e05c9b7a0"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-2045c4a25d"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-recovery-tool](../../../evidence/sources/authorities.md#src-77692cf696) — [some-implementations/riverhog/recovery/pyproject.toml](../../../../../../some-implementations/riverhog/recovery/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-recovery-tool`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5161b9c721ccb8e7abc1154a71d95cbc9941fba4df908c99678d5e2106f02b8c -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_recovery_tool-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_recovery_tool-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Independent recovery tool for Riverhog archives.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-recovery-tool",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "application",
  "source": "some-implementations/riverhog/recovery/pyproject.toml"
}
```

</details>
