# Python distribution: stove0-operator-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-operator-contracts:5054a80799 -->

Canonical public state contracts for the Stove0 v1 operator surface.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e2bc71ee53"></a>
| Concern | Contract |
|---|---|
| <a id="s-d107c8c8b6"></a>`artifacts` | [{"coordinate": "dist/stove0_operator_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_operator_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-eef338aff1"></a>`channel` | github-release |
| <a id="s-3acedf1295"></a>`description` | Canonical public state contracts for the Stove0 v1 operator surface. |
| <a id="s-66e94c7a8a"></a>`license_baseline` | first-v1-publication |
| <a id="s-9e893f9dc4"></a>`license_expression` | Apache-2.0 |
| <a id="s-2f3336bfc9"></a>`publication_identity` | {"coordinate": "stove0-operator-contracts", "kind": "python-distribution"} |
| <a id="s-dcfe4d710a"></a>`requires_python` | >=3.12 |
| <a id="s-e09f9568be"></a>`role` | reusable_library |
| <a id="s-d71c83df6a"></a>`source` | reference/stove0/packages/operator-contracts/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-operator-contracts](../../../evidence/relationships.md#rn-7c400ebb83)

## Governing policies

- <a id="pa-55a133e33f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8166ac3f6e"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-operator-contracts](../../../evidence/sources.md#src-3697a2cd79) — `reference/stove0/packages/operator-contracts/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-operator-contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 79a48d70babff3f189712aff1c094b5b2c90130d03cee1aa20f119fac2f09377 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_operator_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_operator_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Canonical public state contracts for the Stove0 v1 operator surface.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-operator-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/operator-contracts/pyproject.toml"
}
```

</details>
