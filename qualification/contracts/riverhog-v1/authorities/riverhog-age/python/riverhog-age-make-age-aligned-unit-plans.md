# riverhog_age.make_age_aligned_unit_plans

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-make-age-aligned-unit-plans:5ca0139196 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-80d80b9658"></a>
- <a id="s-9e0ab646b2"></a>`distribution`: `riverhog-age`
- <a id="s-81dbc0894e"></a>`module`: `riverhog_age`
- <a id="s-12fdd34c0b"></a>`name`: `make_age_aligned_unit_plans`
- <a id="s-7db1faa08c"></a>`unit`: `export`

### Declared structure

- <a id="s-0cace74000"></a>`kind`: `"function"`
- <a id="s-7d6d7365cd"></a>`signature`: `"\"(plaintext_size: 'int', *, age_prefix_len: 'int', chunks_per_unit: 'int' = 1024) -> 'list[AgeAlignedUnitPlan]'\""`

## Governing policies

- <a id="pa-bc50f22bb2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.make_age_aligned_unit_plans`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea12aedce7bfc3a8baefe22a939c45f8817f896f2bb51334be39ded9c62769f3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plaintext_size: 'int', *, age_prefix_len: 'int', chunks_per_unit: 'int' = 1024) -> 'list[AgeAlignedUnitPlan]'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "make_age_aligned_unit_plans",
  "unit": "export"
}
```
