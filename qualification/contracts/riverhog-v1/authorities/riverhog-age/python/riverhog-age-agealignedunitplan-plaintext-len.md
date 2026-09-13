# riverhog_age.AgeAlignedUnitPlan.plaintext_len

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-agealignedunitplan-plaintext-len:5a8438fc02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67eede741b"></a>
| Field | Shape |
|---|---|
| <a id="s-4ca6923ce4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-43c0a00233"></a>`distribution` | "riverhog-age" |
| <a id="s-ffbe9bbcba"></a>`module` | "riverhog_age" |
| <a id="s-5da201c355"></a>`name` | "plaintext_len" |
| <a id="s-1a551cd34b"></a>`owner` | "riverhog_age.AgeAlignedUnitPlan" |
| <a id="s-8b98990e72"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_age.AgeAlignedUnitPlan](riverhog-age-agealignedunitplan.md)

## Governing policies

- <a id="pa-22f6dd12a9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.AgeAlignedUnitPlan.plaintext_len`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 936c3e4496e1170609632d5ff35263ca2a9386d72f4eedd543f1fee364b3b33a -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'int'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "plaintext_len",
  "owner": "riverhog_age.AgeAlignedUnitPlan",
  "unit": "member"
}
```
