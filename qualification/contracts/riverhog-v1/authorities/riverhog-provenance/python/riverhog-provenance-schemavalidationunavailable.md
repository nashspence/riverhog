# riverhog_provenance.SchemaValidationUnavailable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-schemavalidationunavailable:21bc99d193 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2de3e20d1"></a>
| Field | Shape |
|---|---|
| <a id="s-a0174f3085"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-fc3855be9c"></a>`distribution` | "riverhog-provenance" |
| <a id="s-b03201d420"></a>`module` | "riverhog_provenance" |
| <a id="s-d559e5388e"></a>`name` | "SchemaValidationUnavailable" |
| <a id="s-f9fd3ce684"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f94808df0a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.SchemaValidationUnavailable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56745390cd5235830ab66518bdf949cf020ec25c0a54e39ec3146558ae4978ff -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "SchemaValidationUnavailable",
  "unit": "export"
}
```
