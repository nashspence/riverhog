# Python distribution: stove0-observer-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-observer-support:f25cb7ca6d -->

External-author protocol, runtime, and conformance support for stove0 content observers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-003ab09374"></a>
| Concern | Contract |
|---|---|
| <a id="s-3b04c26417"></a>`artifacts` | `[{"coordinate":"dist/stove0_observer_support-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_observer_support-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-80217009cc"></a>`channel` | `"github-release"` |
| <a id="s-9341b4b5e3"></a>`description` | `"External-author protocol, runtime, and conformance support for stove0 content observers."` |
| <a id="s-7cdfc39d96"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-37004ebfc6"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-a285ba5e82"></a>`publication_identity` | `{"coordinate":"stove0-observer-support","kind":"python-distribution"}` |
| <a id="s-f71635f80b"></a>`requires_python` | `">=3.12"` |
| <a id="s-a58ce03ca5"></a>`role` | `"reusable_library"` |
| <a id="s-8945f9bd59"></a>`source` | `"reference/stove0/packages/observer-support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-observer-support](../../../evidence/relationships.md#rn-50ccd8c774)

## Governing policies

- <a id="pa-ca6c0a3a9b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-2596651a39"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-observer-support](../../../evidence/sources.md#src-49de4b2b28) — `reference/stove0/packages/observer-support/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-observer-support`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 532e319541dcfbbc52c212ebd56d8898b028e5ba62a019d0fdc17924ad969b44 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_observer_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_observer_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "External-author protocol, runtime, and conformance support for stove0 content observers.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-observer-support",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/observer-support/pyproject.toml"
}
```

</details>
