# Python distribution: lifecycle-events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-lifecycle-events:58352114ca -->

Durable CloudEvents lifecycle log and client primitives.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-5e364d3218"></a>
| Concern | Contract |
|---|---|
| <a id="s-814d634c2e"></a>`artifacts` | `[{"coordinate":"dist/lifecycle_events-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/lifecycle_events-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-a9416a0876"></a>`channel` | `"github-release"` |
| <a id="s-7a81146c9a"></a>`description` | `"Durable CloudEvents lifecycle log and client primitives."` |
| <a id="s-9e68ace761"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-3d9209a103"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-ecea441dd3"></a>`publication_identity` | `{"coordinate":"lifecycle-events","kind":"python-distribution"}` |
| <a id="s-fac7b85cb0"></a>`requires_python` | `">=3.12"` |
| <a id="s-c4f789cf6c"></a>`role` | `"reusable_library"` |
| <a id="s-5ebcfff6c7"></a>`source` | `"packages/lifecycle-events/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [lifecycle-events](../../../evidence/relationships/nodes.md#rn-30af2d1593)

## Governing policies

- <a id="pa-6af6d1566f"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-50032d9637"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:lifecycle-events](../../../evidence/sources/authorities.md#src-36336fc3be) — [packages/lifecycle-events/pyproject.toml](../../../../../../packages/lifecycle-events/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/lifecycle-events`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87d915b7475a7692bfc649307ec0b5a8af8b241caeab6686205a8e68b115cb88 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/lifecycle_events-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/lifecycle_events-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Durable CloudEvents lifecycle log and client primitives.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "lifecycle-events",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/lifecycle-events/pyproject.toml"
}
```

</details>
