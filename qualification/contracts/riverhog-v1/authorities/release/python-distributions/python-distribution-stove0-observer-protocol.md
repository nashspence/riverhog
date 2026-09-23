# Python distribution: stove0-observer-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-observer-protocol:1e749cf317 -->

Dependency-light public contracts for external Stove0 content observers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-1fb2121fd2"></a>
| Concern | Contract |
|---|---|
| <a id="s-0367d3bc37"></a>`artifacts` | `[{"coordinate":"dist/stove0_observer_protocol-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_observer_protocol-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-0e6acfa8b5"></a>`channel` | `"github-release"` |
| <a id="s-430672fc88"></a>`description` | `"Dependency-light public contracts for external Stove0 content observers."` |
| <a id="s-6d8ed9dd81"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-95aa45aa31"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-51f0ae21fc"></a>`publication_identity` | `{"coordinate":"stove0-observer-protocol","kind":"python-distribution"}` |
| <a id="s-26e66e614c"></a>`requires_python` | `">=3.12"` |
| <a id="s-3a08d0f376"></a>`role` | `"reusable_library"` |
| <a id="s-a8185be19d"></a>`source` | `"some-implementations/stove0/packages/observer-protocol/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-observer-protocol](../../../evidence/relationships/nodes.md#rn-bac58a079c)

## Governing policies

- <a id="pa-583140e3ba"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-46717115bd"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-observer-protocol](../../../evidence/sources/authorities.md#src-7c927fed76) — [some-implementations/stove0/packages/observer-protocol/pyproject.toml](../../../../../../some-implementations/stove0/packages/observer-protocol/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-observer-protocol`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 818b3893c2d9f2a5fa5d543fefafe2b7df2686d1f130b62054331a1d9db1db3c -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_observer_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_observer_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Dependency-light public contracts for external Stove0 content observers.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-observer-protocol",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/packages/observer-protocol/pyproject.toml"
}
```

</details>
