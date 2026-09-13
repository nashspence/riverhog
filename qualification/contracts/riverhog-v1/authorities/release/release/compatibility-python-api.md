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
- Shape: "Freeze-protected declared public-module exports and public signatures remain backward compatible throughout v1; importable packages explicitly excluded from the Python surface are not Python API promises."

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

<!-- exact-contract-value: 47e0d58419088f26cfb1536ec9e42a06bb328025388b107cbb3f3c9ba83e6a1d -->

```json
"Freeze-protected declared public-module exports and public signatures remain backward compatible throughout v1; importable packages explicitly excluded from the Python surface are not Python API promises."
```
