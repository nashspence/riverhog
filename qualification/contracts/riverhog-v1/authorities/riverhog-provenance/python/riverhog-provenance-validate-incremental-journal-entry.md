# riverhog_provenance.validate_incremental_journal_entry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-validate-incremental-5da1b5dcac:ee4e23304f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-976a747a4c"></a>
| Field | Shape |
|---|---|
| <a id="s-f2fb74f526"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-150c20e099"></a>`distribution` | "riverhog-provenance" |
| <a id="s-518257f9d1"></a>`module` | "riverhog_provenance" |
| <a id="s-3ef699f064"></a>`name` | "validate_incremental_journal_entry" |
| <a id="s-f9fafb8134"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3e28bbdd19"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.validate_incremental_journal_entry`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f8d1f967b9c50ab3c53b7e36247eee04c894683f71551d7844b35261898e71cb -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(encoded: 'bytes', *, sequence: 'int', journal_id: 'str', previous_entry_id: 'str | None', previous_json_sha256: 'str | None') -> 'IncrementalJournalEntry'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "validate_incremental_journal_entry",
  "unit": "export"
}
```
