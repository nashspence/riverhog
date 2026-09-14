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
- <a id="s-6eaba65920"></a>`distribution`: `riverhog-provenance`
- <a id="s-4b341f8d4e"></a>`module`: `riverhog_provenance`
- <a id="s-ffab464123"></a>`name`: `JournalSummary`
- <a id="s-421df73033"></a>`unit`: `export`

### Declared structure

- <a id="s-f71d6d671a"></a>`kind`: `"class"`
- <a id="s-5131e369a2"></a>`signature`: `"\"(journal_id: 'str', primary_lineage_id: 'str', frames: 'tuple[JournalFrame, ...]', entries: 'int', tail_frame: 'JournalFrame', journal_sha256: 'str', current_binding_id: 'str', current_state_id: 'str', current_path: 'str', current_bytes: 'int', current_sha256: 'str', agent_ids: 'frozenset[str]', external_states: 'tuple[ExternalStateReference, ...]') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-c55c6eb92a"></a>`journal_id` | `'str'` | `required` |
| <a id="s-92b7473780"></a>`primary_lineage_id` | `'str'` | `required` |
| <a id="s-f657521f1a"></a>`frames` | `'tuple[JournalFrame, ...]'` | `required` |
| <a id="s-bb7e701ab9"></a>`entries` | `'int'` | `required` |
| <a id="s-11758ba090"></a>`tail_frame` | `'JournalFrame'` | `required` |
| <a id="s-ea65d94b06"></a>`journal_sha256` | `'str'` | `required` |
| <a id="s-a4be47b24b"></a>`current_binding_id` | `'str'` | `required` |
| <a id="s-f9d77f213d"></a>`current_state_id` | `'str'` | `required` |
| <a id="s-b372dbbd14"></a>`current_path` | `'str'` | `required` |
| <a id="s-6e9b9eb72f"></a>`current_bytes` | `'int'` | `required` |
| <a id="s-694ef157bf"></a>`current_sha256` | `'str'` | `required` |
| <a id="s-f6c64b78fe"></a>`agent_ids` | `'frozenset[str]'` | `required` |
| <a id="s-537166ac95"></a>`external_states` | `'tuple[ExternalStateReference, ...]'` | `required` |

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
