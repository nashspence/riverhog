# riverhog_provenance.ProvenanceTerminalDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenanceterminaldocument:eae198a36c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e1b0901c9"></a>
- <a id="s-f3f44e097f"></a>`distribution`: `riverhog-provenance`
- <a id="s-595fd20e74"></a>`module`: `riverhog_provenance`
- <a id="s-cff6f66984"></a>`name`: `ProvenanceTerminalDocument`
- <a id="s-60867fea06"></a>`unit`: `export`

### Declared structure

- <a id="s-4f020bbea7"></a>`kind`: `"class"`
- <a id="s-8925fc373b"></a>`signature`: `"'(archive_generation: \\'str\\', archive_tree_sha256: \\'str\\', sequence: \\'int\\', kind: \"Literal[\\'terminal\\']\" = \\'terminal\\', schema: \\'str\\' = \\'riverhog-provenance-terminal/v1\\') -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-0154483c1a"></a>`archive_generation` | `'str'` | `required` |
| <a id="s-75a53efc6e"></a>`archive_tree_sha256` | `'str'` | `required` |
| <a id="s-e5c8eff3da"></a>`sequence` | `'int'` | `required` |
| <a id="s-c10bedec76"></a>`kind` | `"Literal['terminal']"` | `'terminal'` |
| <a id="s-63a03ebea6"></a>`schema` | `'str'` | `'riverhog-provenance-terminal/v1'` |

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ProvenanceTerminalDocument.to_json_bytes](riverhog-provenance-provenanceterminaldocument-to-json-bytes.md)
- [riverhog_provenance.ProvenanceTerminalDocument.metadata_path](riverhog-provenance-provenanceterminaldocument-metadata-path.md)
- [riverhog_provenance.ProvenanceTerminalDocument.from_json_bytes](riverhog-provenance-provenanceterminaldocument-from-json-bytes.md)
- [riverhog_provenance.ProvenanceTerminalDocument.to_mapping](riverhog-provenance-provenanceterminaldocument-to-mapping.md)

## Governing policies

- <a id="pa-ce67094c2f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceTerminalDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f8df1299631103e37be6ed89f8644bcacccd46b3a3fb454c8adf07ad6edb2f6 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "archive_generation",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "archive_tree_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "sequence",
        "type": "'int'"
      },
      {
        "default": "'terminal'",
        "name": "kind",
        "type": "\"Literal['terminal']\""
      },
      {
        "default": "'riverhog-provenance-terminal/v1'",
        "name": "schema",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "'(archive_generation: \\'str\\', archive_tree_sha256: \\'str\\', sequence: \\'int\\', kind: \"Literal[\\'terminal\\']\" = \\'terminal\\', schema: \\'str\\' = \\'riverhog-provenance-terminal/v1\\') -> None'"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ProvenanceTerminalDocument",
  "unit": "export"
}
```
