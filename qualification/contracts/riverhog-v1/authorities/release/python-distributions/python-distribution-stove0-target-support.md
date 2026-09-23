# Python distribution: stove0-target-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-target-support:e554274152 -->

Hardware-neutral target protocol, runtime, and conformance support for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-3510712c7d"></a>
| Concern | Contract |
|---|---|
| <a id="s-2803e8fc81"></a>`artifacts` | `[{"coordinate":"dist/stove0_target_support-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_target_support-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-173a1aade9"></a>`channel` | `"github-release"` |
| <a id="s-da0d411de2"></a>`description` | `"Hardware-neutral target protocol, runtime, and conformance support for Stove0."` |
| <a id="s-28e808853a"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-977a1b24ac"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-9e0ab37fce"></a>`publication_identity` | `{"coordinate":"stove0-target-support","kind":"python-distribution"}` |
| <a id="s-0af918987f"></a>`requires_python` | `">=3.12"` |
| <a id="s-2c563a70b4"></a>`role` | `"reusable_library"` |
| <a id="s-bee42faebb"></a>`source` | `"some-implementations/stove0/packages/target-support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-target-support](../../../evidence/relationships/nodes.md#rn-f8df68acc8)

## Governing policies

- <a id="pa-ca51937108"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-280efd64c2"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-target-support](../../../evidence/sources/authorities.md#src-745a95dbc1) — [some-implementations/stove0/packages/target-support/pyproject.toml](../../../../../../some-implementations/stove0/packages/target-support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-target-support`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b17048124ddab04ba1f199064b4d5e258f1528dd56a4fd5f7ca664fdc783b02f -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_target_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_target_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Hardware-neutral target protocol, runtime, and conformance support for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-target-support",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/packages/target-support/pyproject.toml"
}
```

</details>
