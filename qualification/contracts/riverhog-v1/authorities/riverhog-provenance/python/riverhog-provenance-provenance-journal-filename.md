# riverhog_provenance.provenance_journal_filename

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-journal-filename:24d267b6d9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d87fc12356"></a>
- <a id="s-c5b22d6548"></a>`distribution`: `riverhog-provenance`
- <a id="s-0c0dbd92d7"></a>`module`: `riverhog_provenance`
- <a id="s-2d3f01df4d"></a>`name`: `provenance_journal_filename`
- <a id="s-0802386656"></a>`unit`: `export`

### Declared structure

- <a id="s-bcf8ca91d9"></a>`kind`: `"function"`
- <a id="s-874bf66a7b"></a>`signature`: `"\"(journal_id: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-da70872353"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.provenance_journal_filename`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c9309951c45d7e1a41e9a21052c2184040744feda8961f38c14cf42451a9ad1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(journal_id: 'str') -> 'str'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "provenance_journal_filename",
  "unit": "export"
}
```
