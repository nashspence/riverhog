# riverhog_provenance.PROVENANCE_BINDING_SEGMENT_FILES_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-binding-se-ed8ce7c8b7:43d3295c74 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c22d8f352"></a>
| Field | Shape |
|---|---|
| <a id="s-479df449e7"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-8ea2fc37ee"></a>`distribution` | "riverhog-provenance" |
| <a id="s-70834e003d"></a>`module` | "riverhog_provenance" |
| <a id="s-3c2b936e95"></a>`name` | "PROVENANCE_BINDING_SEGMENT_FILES_MAX" |
| <a id="s-deb91de7d9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-e97b6d2559"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_BINDING_SEGMENT_FILES_MAX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ced0dba5a7c42e60c7a6236b4f60f23f27c80a5e5c985313d9ddfbb56f02e89 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 512
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_BINDING_SEGMENT_FILES_MAX",
  "unit": "export"
}
```
