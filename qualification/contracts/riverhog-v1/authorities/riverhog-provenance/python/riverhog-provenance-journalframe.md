# riverhog_provenance.JournalFrame

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-journalframe:db58c8b3f8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1a903e0b15"></a>
| Field | Shape |
|---|---|
| <a id="s-3ea849be4f"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-a945304194"></a>`distribution` | "riverhog-provenance" |
| <a id="s-b5303fa8fe"></a>`module` | "riverhog_provenance" |
| <a id="s-1c30d16ead"></a>`name` | "JournalFrame" |
| <a id="s-1ea76144a7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d6ac56a1ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.JournalFrame`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12cf63708b381295319e2d9b0c2149568521b29512db2beb7ff4a9073b63e755 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "sequence",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "json_bytes",
        "type": "'bytes'"
      },
      {
        "default": "required",
        "name": "document",
        "type": "'JsonObject'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(sequence: 'int', json_bytes: 'bytes', document: 'JsonObject', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "JournalFrame",
  "unit": "export"
}
```
