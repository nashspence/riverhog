# riverhog_provenance.PreparedFileProvenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-preparedfileprovenance:8c38d20183 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1bca796d3f"></a>
| Field | Shape |
|---|---|
| <a id="s-b1b24ed0fc"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-9707187dda"></a>`distribution` | "riverhog-provenance" |
| <a id="s-58c7caa074"></a>`module` | "riverhog_provenance" |
| <a id="s-aadbcf941b"></a>`name` | "PreparedFileProvenance" |
| <a id="s-f696784aa6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-26bdc95e0f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.PreparedFileProvenance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2bfc0c6ee1d7f8b378da5e75cf45726512c80f9912c7b723cdef33b3a9fe907e -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "binding",
        "type": "'FileProvenanceBinding'"
      },
      {
        "default": "required",
        "name": "journals",
        "type": "'dict[str, bytes]'"
      },
      {
        "default": "required",
        "name": "source",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(binding: 'FileProvenanceBinding', journals: 'dict[str, bytes]', source: 'str') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PreparedFileProvenance",
  "unit": "export"
}
```
