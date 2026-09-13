# riverhog_provenance.JournalSummary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-journalsummary:e6229fa160 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d35a36d9c4"></a>
| Field | Shape |
|---|---|
| <a id="s-28ebdb2a8a"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-6eaba65920"></a>`distribution` | "riverhog-provenance" |
| <a id="s-4b341f8d4e"></a>`module` | "riverhog_provenance" |
| <a id="s-ffab464123"></a>`name` | "JournalSummary" |
| <a id="s-421df73033"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_provenance.JournalSummary.tail](riverhog-provenance-journalsummary-tail.md)

## Governing policies

- <a id="pa-eb6f3e320a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.JournalSummary`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b22cddea1314f83bd5db72eca85009bce0fd8b49cf1278ef7adf8f9ceaab88b9 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "journal_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "primary_lineage_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "frames",
        "type": "'tuple[JournalFrame, ...]'"
      },
      {
        "default": "required",
        "name": "entries",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "tail_frame",
        "type": "'JournalFrame'"
      },
      {
        "default": "required",
        "name": "journal_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "current_binding_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "current_state_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "current_path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "current_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "current_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "agent_ids",
        "type": "'frozenset[str]'"
      },
      {
        "default": "required",
        "name": "external_states",
        "type": "'tuple[ExternalStateReference, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(journal_id: 'str', primary_lineage_id: 'str', frames: 'tuple[JournalFrame, ...]', entries: 'int', tail_frame: 'JournalFrame', journal_sha256: 'str', current_binding_id: 'str', current_state_id: 'str', current_path: 'str', current_bytes: 'int', current_sha256: 'str', agent_ids: 'frozenset[str]', external_states: 'tuple[ExternalStateReference, ...]') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "JournalSummary",
  "unit": "export"
}
```
