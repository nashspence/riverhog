# Python distribution: a-gogurt-linux-listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-gogurt-linux-listener:944dcfb06b -->

Linux systemd user listener for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-15243edb56"></a>
| Concern | Contract |
|---|---|
| <a id="s-7515e4fdff"></a>`artifacts` | `[{"coordinate":"dist/a_gogurt_linux_listener-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_gogurt_linux_listener-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-10c49a40e1"></a>`channel` | `"github-release"` |
| <a id="s-405ec8e79f"></a>`description` | `"Linux systemd user listener for Gogurt."` |
| <a id="s-691f889d80"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-eb66d54f71"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-c27c43f595"></a>`publication_identity` | `{"coordinate":"a-gogurt-linux-listener","kind":"python-distribution"}` |
| <a id="s-f472ac8a86"></a>`requires_python` | `">=3.12"` |
| <a id="s-dd06ee097f"></a>`role` | `"component"` |
| <a id="s-feef0c6604"></a>`source` | `"some-implementations/gogurt/listener-host/linux/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-gogurt-linux-listener](../../../evidence/relationships/nodes.md#rn-882ba0b60e)

## Governing policies

- <a id="pa-ef034d2a3a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-977935676d"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-gogurt-linux-listener](../../../evidence/sources/authorities.md#src-61ef25e4fa) — [some-implementations/gogurt/listener-host/linux/pyproject.toml](../../../../../../some-implementations/gogurt/listener-host/linux/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-gogurt-linux-listener`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5bf4bbb60a156f4f9a5a41d2b782fbeaf8669ff9d24c971cba95da804cb17b0e -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_gogurt_linux_listener-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_gogurt_linux_listener-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Linux systemd user listener for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-gogurt-linux-listener",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/gogurt/listener-host/linux/pyproject.toml"
}
```

</details>
