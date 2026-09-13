# riverhog_age.DEFAULT_CHUNKS_PER_AGE_UNIT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-default-chunks-per-age-unit:bec625b904 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23692dc3c8"></a>
| Field | Shape |
|---|---|
| <a id="s-6ad28ab554"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-b052f73bc8"></a>`distribution` | "riverhog-age" |
| <a id="s-478980718a"></a>`module` | "riverhog_age" |
| <a id="s-a19a4ced5b"></a>`name` | "DEFAULT_CHUNKS_PER_AGE_UNIT" |
| <a id="s-0bdab5fc9f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-78021a400f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.DEFAULT_CHUNKS_PER_AGE_UNIT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cff9f2411734d704f2a031874c331014bfd8d24dd9fe53f06586faab9f5e8291 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 1024
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "DEFAULT_CHUNKS_PER_AGE_UNIT",
  "unit": "export"
}
```
