# Python distribution: a-gogurt-windows-listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-gogurt-windows-listener:c83e415174 -->

Windows Task Scheduler listener for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-17bb9fd13d"></a>
| Concern | Contract |
|---|---|
| <a id="s-b57e61cb6b"></a>`artifacts` | `[{"coordinate":"dist/a_gogurt_windows_listener-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_gogurt_windows_listener-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-9a4429f1ea"></a>`channel` | `"github-release"` |
| <a id="s-873d8eb8e7"></a>`description` | `"Windows Task Scheduler listener for Gogurt."` |
| <a id="s-997b7bdadc"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-e6780a93c0"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-c85cf3a98e"></a>`publication_identity` | `{"coordinate":"a-gogurt-windows-listener","kind":"python-distribution"}` |
| <a id="s-d14497d4bd"></a>`requires_python` | `">=3.12"` |
| <a id="s-283ad50aba"></a>`role` | `"component"` |
| <a id="s-6fa3135dd2"></a>`source` | `"some-implementations/gogurt/listener-host/windows/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-gogurt-windows-listener](../../../evidence/relationships/nodes.md#rn-c28b071ce2)

## Governing policies

- <a id="pa-0dfa25bc5b"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-c104d61640"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-gogurt-windows-listener](../../../evidence/sources/authorities.md#src-c32f350e12) — [some-implementations/gogurt/listener-host/windows/pyproject.toml](../../../../../../some-implementations/gogurt/listener-host/windows/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-gogurt-windows-listener`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8973e5ec45ac538c5c2771b6558cc45527472e6d7a0d114b1ca574b69280f71d -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_gogurt_windows_listener-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_gogurt_windows_listener-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Windows Task Scheduler listener for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-gogurt-windows-listener",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/gogurt/listener-host/windows/pyproject.toml"
}
```

</details>
