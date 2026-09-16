# Compatibility: python api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-python-api:20c622516a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="s-7d6f9abc79"></a>
- Shape: "Freeze-protected declared public-module exports, callable signatures, selected constants, enum members and values, and selected public model and dataclass structures remain backward compatible throughout v1."

## Governing policies

- <a id="pa-488965c306"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/python_api`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e36c233d9c32bdf67be4f55acc6a0fa07e262c5d5e73466ce68307d445bdf4f -->

```json
"Freeze-protected declared public-module exports, callable signatures, selected constants, enum members and values, and selected public model and dataclass structures remain backward compatible throughout v1."
```
