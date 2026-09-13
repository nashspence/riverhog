# riverhog_provenance.IncrementalJournalEntry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-incrementaljournalentry:d5194ae59e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6da2b1c45"></a>
| Field | Shape |
|---|---|
| <a id="s-2edb3c9b4a"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-23cd7fdc5b"></a>`distribution` | "riverhog-provenance" |
| <a id="s-e6ce3e095e"></a>`module` | "riverhog_provenance" |
| <a id="s-5dae5de5c8"></a>`name` | "IncrementalJournalEntry" |
| <a id="s-713e1bf334"></a>`unit` | "export" |

## Governing policies

- <a id="pa-50601393c8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.IncrementalJournalEntry`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e478524f2a23dfbdaa1f1e23808908d06995bd2b98c06358963cfbee42aa625f -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "frame",
        "type": "'JournalFrame'"
      },
      {
        "default": "required",
        "name": "journal_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "primary_lineage_id",
        "type": "'str | None'"
      },
      {
        "default": "required",
        "name": "agents",
        "type": "'tuple[str, ...]'"
      },
      {
        "default": "required",
        "name": "events",
        "type": "'tuple[str, ...]'"
      },
      {
        "default": "required",
        "name": "states",
        "type": "'tuple[tuple[str, str], ...]'"
      },
      {
        "default": "required",
        "name": "entities",
        "type": "'tuple[tuple[str, str, str], ...]'"
      },
      {
        "default": "required",
        "name": "entity_counts",
        "type": "'tuple[tuple[str, int], ...]'"
      },
      {
        "default": "required",
        "name": "bindings",
        "type": "'tuple[tuple[str, str, str], ...]'"
      },
      {
        "default": "required",
        "name": "external_states",
        "type": "'tuple[ExternalStateReference, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(frame: 'JournalFrame', journal_id: 'str', primary_lineage_id: 'str | None', agents: 'tuple[str, ...]', events: 'tuple[str, ...]', states: 'tuple[tuple[str, str], ...]', entities: 'tuple[tuple[str, str, str], ...]', entity_counts: 'tuple[tuple[str, int], ...]', bindings: 'tuple[tuple[str, str, str], ...]', external_states: 'tuple[ExternalStateReference, ...]') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "IncrementalJournalEntry",
  "unit": "export"
}
```
