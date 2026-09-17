# riverhog_provenance.DerivativeJournalSeed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-derivativejournalseed:855541e80e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e4079f440"></a>
- <a id="s-16b0f96430"></a>`distribution`: `riverhog-provenance`
- <a id="s-e565075394"></a>`module`: `riverhog_provenance`
- <a id="s-7209c445e1"></a>`name`: `DerivativeJournalSeed`
- <a id="s-a294f73ecb"></a>`unit`: `export`

### Declared structure

- <a id="s-083c1a07a0"></a>`kind`: `"class"`
- <a id="s-6f2a675f94"></a>`signature`: `"\"(journal_id: 'str', recorded_by_agent_id: 'str', state_id: 'str', activity_id: 'str', current_entry_id: 'str', current_entry_json_sha256: 'str', previous_entry_id: 'str', previous_entry_json_sha256: 'str', next_sequence: 'int') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-aa4195957b"></a>`journal_id` | `'str'` | `required` |
| <a id="s-8b889bc21a"></a>`recorded_by_agent_id` | `'str'` | `required` |
| <a id="s-f47b62ded7"></a>`state_id` | `'str'` | `required` |
| <a id="s-7655442f91"></a>`activity_id` | `'str'` | `required` |
| <a id="s-975798b7e6"></a>`current_entry_id` | `'str'` | `required` |
| <a id="s-bd04447964"></a>`current_entry_json_sha256` | `'str'` | `required` |
| <a id="s-d7ac409799"></a>`previous_entry_id` | `'str'` | `required` |
| <a id="s-9a94c097f8"></a>`previous_entry_json_sha256` | `'str'` | `required` |
| <a id="s-8869b7c89c"></a>`next_sequence` | `'int'` | `required` |

## Governing policies

- <a id="pa-36233ada0f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.DerivativeJournalSeed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ef5bf5389886c054245c52a6b9553d738b1f6f421a45265e0a6014a9ca250d7 -->

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
        "name": "recorded_by_agent_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "state_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "activity_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "current_entry_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "current_entry_json_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "previous_entry_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "previous_entry_json_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "next_sequence",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "\"(journal_id: 'str', recorded_by_agent_id: 'str', state_id: 'str', activity_id: 'str', current_entry_id: 'str', current_entry_json_sha256: 'str', previous_entry_id: 'str', previous_entry_json_sha256: 'str', next_sequence: 'int') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "DerivativeJournalSeed",
  "unit": "export"
}
```

</details>
