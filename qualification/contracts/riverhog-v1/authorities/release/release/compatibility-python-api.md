# Compatibility: python api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-python-api:112aef323a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [compatibility](index.md#f-6df58a8f93) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7d6f9abc79"></a>
- Shape: "Reusable-library declared public-module exports and public signatures remain backward compatible throughout v1; undeclared implementation submodules and other component roles are not Python API promises."

## Governing policies

- <a id="pa-cb2867544c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/python_api`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ecf544dd59d5ef2c7fd2d9914ab0230ea456a75ae18045efa13467056c0c9287 -->

```json
"Reusable-library declared public-module exports and public signatures remain backward compatible throughout v1; undeclared implementation submodules and other component roles are not Python API promises."
```
