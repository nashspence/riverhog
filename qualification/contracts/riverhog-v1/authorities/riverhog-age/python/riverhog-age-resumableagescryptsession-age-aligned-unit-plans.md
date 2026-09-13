# riverhog_age.ResumableAgeScryptSession.age_aligned_unit_plans

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-ag-4597dee74d:740a8a87c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7550a785e"></a>
| Field | Shape |
|---|---|
| <a id="s-320cc2b15e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-808af29f2c"></a>`distribution` | "riverhog-age" |
| <a id="s-fc73719f0d"></a>`module` | "riverhog_age" |
| <a id="s-e4327dd963"></a>`name` | "age_aligned_unit_plans" |
| <a id="s-39a33f1986"></a>`owner` | "riverhog_age.ResumableAgeScryptSession" |
| <a id="s-3f0b64c513"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_age.ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-2f0def3097"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.age_aligned_unit_plans`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22c770c16e3f54a9d87f9111f71231772c75a6013a51d9d295bdea405d10df5e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plaintext_size: 'int', *, chunks_per_unit: 'int' = 1024) -> 'list[AgeAlignedUnitPlan]'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "age_aligned_unit_plans",
  "owner": "riverhog_age.ResumableAgeScryptSession",
  "unit": "member"
}
```
