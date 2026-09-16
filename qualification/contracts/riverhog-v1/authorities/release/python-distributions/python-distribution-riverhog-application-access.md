# Python distribution: riverhog-application-access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-application-access:8724b6c164 -->

Public Riverhog application-access contracts and canonical grant grammar.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ebe0551fd9"></a>
| Concern | Contract |
|---|---|
| <a id="s-d2555c9436"></a>`artifacts` | [{"coordinate": "dist/riverhog_application_access-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_application_access-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-8ba886f13a"></a>`channel` | github-release |
| <a id="s-ee2bc70ad6"></a>`description` | Public Riverhog application-access contracts and canonical grant grammar. |
| <a id="s-a098e7a06a"></a>`license_baseline` | first-v1-publication |
| <a id="s-ec857c1b66"></a>`license_expression` | Apache-2.0 |
| <a id="s-e211778d8f"></a>`publication_identity` | {"coordinate": "riverhog-application-access", "kind": "python-distribution"} |
| <a id="s-ed5dcfde56"></a>`requires_python` | >=3.12 |
| <a id="s-60c1d554ee"></a>`role` | reusable_library |
| <a id="s-c1c491a3e6"></a>`source` | packages/riverhog-application-access/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-application-access](../../../evidence/relationships.md#rn-06ef7f89ca)

## Governing policies

- <a id="pa-71c52e1216"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-4a8f2a8840"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-application-access](../../../evidence/sources.md#src-1524c1360d) — `packages/riverhog-application-access/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-application-access`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d07fa6d9931e47e7625068f1f20940a635a14df651e3f36c619b1afb040953ca -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_application_access-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_application_access-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Public Riverhog application-access contracts and canonical grant grammar.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-application-access",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-application-access/pyproject.toml"
}
```

</details>
